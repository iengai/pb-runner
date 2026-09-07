//! Bybit v5 request signing (`X-BAPI-SIGN-TYPE: 2`, HMAC-SHA256).
//!
//! `sign = hex(HMAC_SHA256(secret, timestamp + api_key + recv_window + payload))`
//! where `payload` is the query string for GET and the raw JSON body for POST
//! (ccxt `bybit.sign`).

use hmac::{Hmac, Mac};
use sha2::Sha256;

pub fn signature(
    secret: &str,
    timestamp_ms: u64,
    api_key: &str,
    recv_window_ms: u64,
    payload: &str,
) -> String {
    let mut mac =
        Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("hmac accepts any key length");
    mac.update(timestamp_ms.to_string().as_bytes());
    mac.update(api_key.as_bytes());
    mac.update(recv_window_ms.to_string().as_bytes());
    mac.update(payload.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

#[cfg(test)]
mod tests {
    use super::signature;

    // Vectors computed with Python `hmac.new(secret, ts+key+rw+payload, sha256)`.
    #[test]
    fn get_query_vector() {
        let s = signature(
            "secret-key-for-tests",
            1_700_000_000_000,
            "api-key-for-tests",
            5000,
            "category=linear&settleCoin=USDT&limit=200",
        );
        assert_eq!(
            s,
            "df07abbbd26e9f04bbe61fec1193875ab27de8cac6970178131cc9df1e6e28b4"
        );
    }

    #[test]
    fn post_body_vector() {
        let s = signature(
            "secret-key-for-tests",
            1_700_000_000_000,
            "api-key-for-tests",
            5000,
            r#"{"category":"linear","symbol":"BTCUSDT"}"#,
        );
        assert_eq!(
            s,
            "e0be131cecad4b6587f4536d1c7f4362b92d64286b2a047d7a00a4b3097667c3"
        );
    }
}
