//! The HTTP proxy SoundCloud's requests go through when the service is blocked where the
//! listener is. There is one address for the whole app, set from settings, and an empty one
//! leaves the system's proxy settings in charge.
//!
//! The sign-in window needs the proxy too, but a browser is pointed at one by address alone: it
//! drops the user and password a `http://user:pass@host:port` address carries and the page comes
//! up as a proxy error. So the window is given a relay on the loopback instead, which puts the
//! header on every request and forwards it to the saved proxy.

use std::io::{self, Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::sync::{Mutex, RwLock};
use std::thread;
use std::time::Duration;

use anyhow::{Context as _, Result, bail};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use reqwest::ClientBuilder;

static PROXY: RwLock<Option<String>> = RwLock::new(None);

/// The relay [`webview`] last started, kept for the life of the process so a second window reuses
/// the listener. A changed proxy replaces it and leaves the old one to the OS.
static RELAY: Mutex<Option<Relay>> = Mutex::new(None);

/// How long a connection may take to send or answer a head, so a silent peer does not hold a
/// thread for ever. An established tunnel is left without one.
const HEAD: Duration = Duration::from_secs(10);
/// The most a head may run to, so a peer that never sends the blank line cannot grow it without
/// bound.
const HEAD_LIMIT: usize = 32 * 1024;

/// A loopback listener standing in for the saved proxy, so the window can name it.
struct Relay {
    /// The proxy address this relay forwards to. A different address means a different relay.
    upstream: String,
    port: u16,
}

impl Relay {
    /// The address to point a browser at, without credentials: the relay carries those itself.
    fn address(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }
}

/// Sets the proxy every SoundCloud request goes through, or clears it when `proxy` is empty.
pub fn set(proxy: Option<&str>) {
    let proxy = proxy
        .map(str::trim)
        .filter(|proxy| !proxy.is_empty())
        .map(str::to_owned);
    *PROXY
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = proxy;
}

/// Whether `address` is one [`set`] can use: empty to leave the system's proxy settings in
/// charge, or an `http://` or `https://` address with a host. Only an address that passes
/// replaces the saved proxy, so a half-typed one leaves the working one in place.
pub fn valid(address: &str) -> bool {
    let address = address.trim();
    if address.is_empty() {
        return true;
    }
    let Ok(url) = reqwest::Url::parse(address) else {
        return false;
    };
    matches!(url.scheme(), "http" | "https") && url.host_str().is_some()
}

/// Adds the saved proxy to `builder`, leaving it untouched when there is none or the address
/// does not parse. A bad setting must never keep a provider from reaching the network at all.
pub fn apply(builder: ClientBuilder) -> ClientBuilder {
    let address = PROXY
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone();
    let Some(address) = address else {
        return builder;
    };
    match reqwest::Proxy::all(&address) {
        Ok(proxy) => builder.proxy(proxy),
        Err(error) => {
            log::warn!("proxy: cannot use {address}: {error}");
            builder
        }
    }
}

/// The address to point a browser window at, or `None` when SoundCloud goes out directly. The
/// window cannot carry the saved proxy's credentials, so the answer is a relay on the loopback
/// that adds them; the relay is started on the first call and reused afterwards.
pub fn webview() -> Option<String> {
    let upstream = PROXY
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone()?;
    let mut relay = RELAY
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if relay.as_ref().map(|relay| relay.upstream.as_str()) != Some(upstream.as_str()) {
        *relay = match listen(&upstream) {
            Ok(port) => Some(Relay { upstream, port }),
            Err(error) => {
                log::warn!("proxy: cannot open the sign-in relay: {error}");
                None
            }
        };
    }
    relay.as_ref().map(Relay::address)
}

/// Starts a relay for `upstream` and returns the port it listens on. Each connection gets a thread
/// of its own: a browser keeps several open at once and one slow answer must not stall the rest.
fn listen(upstream: &str) -> Result<u16> {
    let url = reqwest::Url::parse(upstream).context("the proxy address does not parse")?;
    let host = url
        .host_str()
        .context("the proxy address has no host")?
        .to_owned();
    let port = url
        .port_or_known_default()
        .context("the proxy address has no port")?;
    let credentials = match (url.username(), url.password()) {
        ("", _) => None,
        (user, password) => {
            Some(STANDARD.encode(format!("{user}:{}", password.unwrap_or_default())))
        }
    };
    let listener =
        TcpListener::bind(("127.0.0.1", 0)).context("cannot listen on the loopback address")?;
    let local = listener
        .local_addr()
        .context("cannot read the relay's address")?
        .port();
    thread::Builder::new()
        .name("proxy-relay".to_owned())
        .spawn(move || {
            for client in listener.incoming().flatten() {
                let host = host.clone();
                let credentials = credentials.clone();
                let served = thread::Builder::new()
                    .name("proxy-relay-link".to_owned())
                    .spawn(move || {
                        if let Err(error) = forward(client, &host, port, credentials.as_deref()) {
                            log::debug!("proxy: a sign-in relay connection ended: {error}");
                        }
                    });
                if let Err(error) = served {
                    log::warn!("proxy: cannot serve a sign-in relay connection: {error}");
                }
            }
        })
        .context("cannot start the sign-in relay")?;
    Ok(local)
}

/// Serves one client connection: opens the matching connection to the proxy, adds the credentials
/// and joins the two. An `https` page arrives as one `CONNECT`; a plain `http` one is forwarded
/// with its head rewritten. Everything after that is copied through untouched.
fn forward(mut client: TcpStream, host: &str, port: u16, credentials: Option<&str>) -> Result<()> {
    let _ = client.set_read_timeout(Some(HEAD));
    let Some(head) = read_head(&mut client)? else {
        return Ok(());
    };
    let Some((method, target)) = request_line(&head) else {
        return reply(&mut client, "HTTP/1.1 400 Bad Request\r\n\r\n");
    };
    let mut server = match TcpStream::connect((host, port)) {
        Ok(server) => server,
        Err(error) => {
            log::warn!("proxy: cannot reach {host}:{port}: {error}");
            return reply(&mut client, "HTTP/1.1 502 Bad Gateway\r\n\r\n");
        }
    };
    let _ = server.set_read_timeout(Some(HEAD));
    let authorization = credentials
        .map(|credentials| format!("Proxy-Authorization: Basic {credentials}\r\n"))
        .unwrap_or_default();

    if method.eq_ignore_ascii_case("CONNECT") {
        write!(
            server,
            "CONNECT {target} HTTP/1.1\r\nHost: {target}\r\n{authorization}\r\n"
        )
        .context("cannot ask the proxy to open the tunnel")?;
        server
            .flush()
            .context("cannot send the proxy the tunnel request")?;
        let Some(answer) = read_head(&mut server)? else {
            return reply(&mut client, "HTTP/1.1 502 Bad Gateway\r\n\r\n");
        };
        if !answer.starts_with("HTTP/1.1 200") && !answer.starts_with("HTTP/1.0 200") {
            // The proxy's own refusal goes back as it came, so the window shows what went wrong.
            client
                .write_all(answer.as_bytes())
                .context("cannot pass on the proxy's refusal")?;
            return client
                .flush()
                .context("cannot send the window the proxy's refusal");
        }
        client
            .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
            .context("cannot confirm the tunnel to the window")?;
        client
            .flush()
            .context("cannot send the window the tunnel")?;
    } else {
        // A browser already sends the full url on the request line, so the head only needs the
        // header. Later requests on a kept-alive plain connection go without it, which costs
        // nothing on a service reached over `https`.
        server
            .write_all(rewrite(&head, &authorization).as_bytes())
            .context("cannot send the proxy the request")?;
    }
    tunnel(client, server)
}

/// Copies both directions of an established connection until each side closes its own.
fn tunnel(client: TcpStream, server: TcpStream) -> Result<()> {
    let _ = client.set_read_timeout(None);
    let _ = server.set_read_timeout(None);
    let mut from_client = client
        .try_clone()
        .context("cannot split the window's link")?;
    let mut from_server = server
        .try_clone()
        .context("cannot split the proxy's link")?;
    let mut to_server = server;
    let mut to_client = client;
    let flowing = thread::Builder::new()
        .name("proxy-relay-flow".to_owned())
        .spawn(move || {
            let _ = io::copy(&mut from_client, &mut to_server);
            let _ = to_server.shutdown(Shutdown::Write);
        })
        .context("cannot start a relay direction")?;
    let _ = io::copy(&mut from_server, &mut to_client);
    let _ = to_client.shutdown(Shutdown::Write);
    let _ = flowing.join();
    Ok(())
}

/// Reads one request head up to and including its blank line, a byte at a time so nothing past it
/// is swallowed. `None` means the peer closed before sending anything.
fn read_head(stream: &mut TcpStream) -> Result<Option<String>> {
    let mut head = Vec::new();
    let mut byte = [0u8; 1];
    while head.len() < HEAD_LIMIT {
        match stream.read(&mut byte) {
            Ok(0) => return Ok(None),
            Ok(_) => head.push(byte[0]),
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error).context("cannot read a head"),
        }
        if head.ends_with(b"\r\n\r\n") {
            let head = String::from_utf8(head).context("a head is not text")?;
            return Ok(Some(head));
        }
    }
    bail!("a head ran past {HEAD_LIMIT} bytes")
}

/// The method and the request target of a head, or `None` when the first line is not a request
/// line at all.
fn request_line(head: &str) -> Option<(&str, &str)> {
    let mut words = head.lines().next()?.split_whitespace();
    Some((words.next()?, words.next()?))
}

/// Puts the credentials in front of the blank line that ends `head`.
fn rewrite(head: &str, authorization: &str) -> String {
    if authorization.is_empty() {
        return head.to_owned();
    }
    let trimmed = head.trim_end_matches("\r\n\r\n");
    format!("{trimmed}\r\n{authorization}\r\n")
}

/// Answers the window without opening a connection to the proxy, for a request that cannot go
/// anywhere.
fn reply(client: &mut TcpStream, answer: &str) -> Result<()> {
    client
        .write_all(answer.as_bytes())
        .context("cannot answer the window")?;
    client.flush().context("cannot send the window the answer")
}
