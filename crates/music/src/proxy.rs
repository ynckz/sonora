//! The HTTP proxy SoundCloud's requests go through when the service is blocked where the
//! listener is. There is one address for the whole app, set from settings, and an empty one
//! leaves the system's proxy settings in charge.

use std::sync::RwLock;

use reqwest::ClientBuilder;

static PROXY: RwLock<Option<String>> = RwLock::new(None);

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
