//! Hand-written Bybit v5 REST client for USDT-linear perpetuals (D11).
//!
//! Every request shape and every field read mirrors the code path passivbot's
//! Python adapter takes through ccxt (docs/PORT_INVENTORY.md section 3), so
//! that the runner sees the same account state the Python bot would see.

pub mod parse;
pub mod sign;

use crate::{
    Balance, CancelAck, Candle, ClosedPnl, ExchangeClient, ExchangeError, Fill, MarginMode,
    MarketSpec, NewOrder, OpenOrder, OrderResult, Position, PositionSide, Side, Ticker,
};
use async_trait::async_trait;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::RwLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub const MAINNET: &str = "https://api.bybit.com";
pub const TESTNET: &str = "https://api-testnet.bybit.com";

/// passivbot's Bybit broker code (`broker_codes.hjson`), which
/// `exchanges/bybit.py::create_ccxt_sessions` puts in ccxt's
/// `options["brokerId"]` -- and ccxt then sends as the `Referer` header on
/// every POST. passivbot treats it as mandatory: a missing or non-string
/// code raises before the bot starts. We are running passivbot's engine on
/// passivbot's configs, so we identify ourselves the same way; the Python
/// bot on these same accounts always has.
///
/// It is not only attribution. Bybit waives the per-symbol
/// `minNotionalValue` for orders that carry it (D25, measured -- undocumented
/// anywhere we could find), so dropping it costs the ability to open a
/// position on any account whose initial entry falls under that minimum.
/// Bybit's own broker FAQ says rebates are computed per tagged ORDER, so
/// this string also decides who earns from the volume: see D26.
pub const DEFAULT_BROKER_ID: &str = "passivbotbybit";

/// Return codes the Python adapter treats as "already in the requested
/// state" (`exchanges/bybit.py::update_exchange_config*`).
const NOT_MODIFIED_CODES: &[&str] = &["110025", "110026", "110043"];
/// Cancel errors the Python adapter treats as "order already gone"
/// (`passivbot.py::execute_cancellation`).
const ALREADY_GONE_CODES: &[&str] = &["110001"];
const ALREADY_GONE_PATTERNS: &[&str] = &[
    "order not exists",
    "order does not exist",
    "order not found",
    "too late to cancel",
    "already filled",
    "already cancelled",
    "already canceled",
];
const AUTH_CODES: &[&str] = &[
    "10003", "10004", "10005", "10007", "10008", "10009", "10010", "33004",
];
const RATE_LIMIT_CODES: &[&str] = &["10006", "10016", "10018"];

pub const WEEK_MS: u64 = 7 * 24 * 60 * 60 * 1000;
/// `exchanges/bybit.py::fetch_fills`: `end_time = exchange_time + 4h` when
/// no end is given; the loop is bounded to 100 fetches.
pub const FILLS_END_SLACK_MS: u64 = 4 * 60 * 60 * 1000;
pub const FILLS_MAX_WINDOWS: usize = 100;
/// `exchanges/bybit.py::fetch_pnls_sub`: `end_time = exchange_time + 1d`;
/// at most 52 weekly windows (one year).
pub const PNL_END_SLACK_MS: u64 = 24 * 60 * 60 * 1000;
pub const PNL_MAX_WINDOWS: usize = 52;

/// Explicit `[startTime, endTime]` windows of at most 7 days covering
/// `[start_ms, end_ms]`, newest first, as the Python adapter walks them:
/// `fetch_fills` (bybit.py:279-308) pages backwards by `endTime` (Bybit
/// returns `[endTime - 7d, endTime]` when only `endTime` is sent, bybit.py:
/// 495-500), `fetch_pnls_sub` (bybit.py:200-225) computes
/// `sts = end - week*i, ets = sts + week, sts = max(sts, start)` and stops
/// once `sts <= start`. Both are the same arithmetic; the runner sends both
/// bounds explicitly. Empty windows are walked through (Python's
/// `fetch_fills` stops at the first empty week, D19), so a lookback with a
/// quiet recent week still loads completely.
pub fn weekly_windows(start_ms: u64, end_ms: u64, max_windows: usize) -> Vec<(u64, u64)> {
    let mut out = Vec::new();
    if end_ms <= start_ms {
        return out;
    }
    let mut end = end_ms;
    for _ in 0..max_windows {
        let start = end.saturating_sub(WEEK_MS).max(start_ms);
        out.push((start, end));
        if start <= start_ms {
            break;
        }
        end = start;
    }
    out
}

/// Fee rates ccxt 4.5.66 seeds into `markets[symbol]` for Bybit linear
/// contracts (`describe().fees`; the Python bot reads `maker`/`taker` from
/// there; verified against the live account on 2026-09-07, P3.4).
pub const DEFAULT_MAKER_FEE: f64 = 0.0001;
pub const DEFAULT_TAKER_FEE: f64 = 0.0006;

#[derive(Debug, Clone)]
pub struct BybitConfig {
    pub api_key: String,
    pub secret: String,
    pub base_url: String,
    /// Sent as `Referer` on every POST. Empty means send no header at all,
    /// which is a deliberate choice with a cost -- see `DEFAULT_BROKER_ID`.
    pub broker_id: String,
    pub recv_window_ms: u64,
    pub timeout: Duration,
    /// Passed as `timeInForce: PostOnly` when an order has `post_only`;
    /// otherwise `GTC` (Python: `live.time_in_force`).
    pub maker_fee: f64,
    pub taker_fee: f64,
}

impl BybitConfig {
    pub fn mainnet(api_key: impl Into<String>, secret: impl Into<String>) -> Self {
        Self {
            api_key: api_key.into(),
            secret: secret.into(),
            base_url: MAINNET.to_string(),
            broker_id: DEFAULT_BROKER_ID.to_string(),
            recv_window_ms: 5000,
            timeout: Duration::from_secs(30),
            maker_fee: DEFAULT_MAKER_FEE,
            taker_fee: DEFAULT_TAKER_FEE,
        }
    }
}

pub struct BybitClient {
    cfg: BybitConfig,
    http: reqwest::Client,
    /// exchange id -> unified symbol, filled by `load_markets`.
    ids: RwLock<HashMap<String, String>>,
    /// unified symbol -> market spec (for qty/price formatting).
    markets: RwLock<HashMap<String, MarketSpec>>,
    /// ccxt `options.enableUnifiedMargin || options.enableUnifiedAccount`
    /// (`is_unified_enabled`), resolved once from `/v5/user/query-api`.
    unified: RwLock<Option<bool>>,
    /// `server_time - local_time`, added to every signed request's timestamp.
    /// ccxt's `options.adjustForTimeDifference`, which the Python bot enables
    /// (`_build_ccxt_options`, passivbot.py:2511-2512) and re-loads whenever a
    /// timestamp error comes back (`_maybe_recover_exchange_time_sync`).
    /// Bybit rejects a request whose timestamp is more than 1 s AHEAD of its
    /// server (`recv_window` only bounds lateness), so a host clock running a
    /// second fast fails every signed call — and each failure is a cycle
    /// error, which walks the runner straight into its restart budget.
    time_offset_ms: AtomicI64,
}

fn system_now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn query_string(params: &[(&str, String)]) -> String {
    params
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&")
}

/// Classify a non-zero `retCode` the way passivbot/ccxt consumers branch on it.
fn classify(code: &str, msg: &str) -> ExchangeError {
    if RATE_LIMIT_CODES.contains(&code) {
        return ExchangeError::RateLimited {
            retry_after_ms: 1000,
        };
    }
    if AUTH_CODES.contains(&code) {
        return ExchangeError::Auth(format!("{code} {msg}"));
    }
    ExchangeError::Rejected {
        code: code.to_string(),
        msg: msg.to_string(),
    }
}

fn is_already_gone(err: &ExchangeError) -> bool {
    match err {
        ExchangeError::Rejected { code, msg } => {
            let m = msg.to_ascii_lowercase();
            ALREADY_GONE_CODES.contains(&code.as_str())
                || ALREADY_GONE_PATTERNS.iter().any(|p| m.contains(p))
        }
        _ => false,
    }
}

fn is_not_modified(err: &ExchangeError) -> bool {
    match err {
        ExchangeError::Rejected { code, msg } => {
            NOT_MODIFIED_CODES.contains(&code.as_str())
                || msg.to_ascii_lowercase().contains("not modified")
        }
        _ => false,
    }
}

impl BybitClient {
    pub fn new(cfg: BybitConfig) -> Result<Self, ExchangeError> {
        let http = reqwest::Client::builder()
            .timeout(cfg.timeout)
            .gzip(true)
            .build()
            .map_err(|e| ExchangeError::Other(format!("http client: {e}")))?;
        Ok(Self {
            cfg,
            http,
            ids: RwLock::new(HashMap::new()),
            markets: RwLock::new(HashMap::new()),
            unified: RwLock::new(None),
            time_offset_ms: AtomicI64::new(0),
        })
    }

    /// ccxt `bybit.is_unified_enabled()`: `GET /v5/user/query-api`,
    /// `unified == 1 || uta == 1` (`enableUnifiedMargin` / `enableUnifiedAccount`).
    /// Cached for the life of the client as ccxt caches it in `options`.
    pub async fn is_unified_account(&self) -> Result<bool, ExchangeError> {
        if let Some(u) = self.unified.read().ok().and_then(|g| *g) {
            return Ok(u);
        }
        let result = self.private_get("/v5/user/query-api", &[]).await?;
        let flag = |k: &str| {
            result.get(k).and_then(|v| {
                v.as_i64()
                    .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
            }) == Some(1)
        };
        let unified = flag("unified") || flag("uta");
        if let Ok(mut g) = self.unified.write() {
            *g = Some(unified);
        }
        Ok(unified)
    }

    pub fn config(&self) -> &BybitConfig {
        &self.cfg
    }

    fn symbol_of(&self, id: &str) -> Option<String> {
        self.ids.read().ok()?.get(id).cloned()
    }

    fn market(&self, symbol: &str) -> Result<MarketSpec, ExchangeError> {
        self.markets
            .read()
            .ok()
            .and_then(|m| m.get(symbol).cloned())
            .ok_or_else(|| {
                ExchangeError::Other(format!("unknown market {symbol}; call load_markets first"))
            })
    }

    fn id_of(&self, symbol: &str) -> Result<String, ExchangeError> {
        parse::exchange_id(symbol).ok_or_else(|| {
            ExchangeError::NotSupported(format!("symbol {symbol} is not a USDT linear perpetual"))
        })
    }

    /// Unwrap the v5 envelope `{retCode, retMsg, result}`.
    fn unwrap_envelope(body: Value) -> Result<Value, ExchangeError> {
        let code = match body.get("retCode") {
            Some(Value::Number(n)) => n.to_string(),
            Some(Value::String(s)) => s.clone(),
            _ => return Err(ExchangeError::Malformed(format!("no retCode in {body}"))),
        };
        let msg = body
            .get("retMsg")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        if code != "0" {
            return Err(classify(&code, &msg));
        }
        Ok(body.get("result").cloned().unwrap_or(Value::Null))
    }

    async fn send(&self, req: reqwest::RequestBuilder) -> Result<Value, ExchangeError> {
        let resp = req
            .send()
            .await
            .map_err(|e| ExchangeError::Network(e.to_string()))?;
        let status = resp.status();
        let text = resp
            .text()
            .await
            .map_err(|e| ExchangeError::Network(e.to_string()))?;
        if status.as_u16() == 429 || status.as_u16() == 403 {
            return Err(ExchangeError::RateLimited {
                retry_after_ms: 1000,
            });
        }
        if status.is_server_error() {
            return Err(ExchangeError::Network(format!(
                "http {status}: {}",
                text.chars().take(200).collect::<String>()
            )));
        }
        let body: Value = serde_json::from_str(&text).map_err(|e| {
            ExchangeError::Malformed(format!(
                "{e}: {}",
                text.chars().take(200).collect::<String>()
            ))
        })?;
        Self::unwrap_envelope(body)
    }

    pub async fn public_get(
        &self,
        path: &str,
        params: &[(&str, String)],
    ) -> Result<Value, ExchangeError> {
        let url = format!("{}{}?{}", self.cfg.base_url, path, query_string(params));
        self.send(self.http.get(url)).await
    }

    /// Local clock corrected by the last `sync_time` (0 until it first runs).
    pub fn now_ms(&self) -> u64 {
        let local = system_now_ms() as i64;
        (local + self.time_offset_ms.load(Ordering::Relaxed)).max(0) as u64
    }

    pub fn time_offset_ms(&self) -> i64 {
        self.time_offset_ms.load(Ordering::Relaxed)
    }

    /// ccxt `load_time_difference`: read the exchange clock and store
    /// `server - local`, measured at the midpoint of the request so the
    /// round trip does not land in the offset. Public endpoint, no signing —
    /// it works even while the clock is too skewed for a signed call.
    pub async fn sync_time(&self) -> Result<i64, ExchangeError> {
        let t0 = system_now_ms() as i64;
        let result = self.public_get("/v5/market/time", &[]).await?;
        let t1 = system_now_ms() as i64;
        let server = result
            .get("timeNano")
            .and_then(Value::as_str)
            .and_then(|s| s.parse::<i128>().ok())
            .map(|nano| (nano / 1_000_000) as i64)
            .or_else(|| {
                result
                    .get("timeSecond")
                    .and_then(Value::as_str)
                    .and_then(|s| s.parse::<i64>().ok())
                    .map(|sec| sec * 1000)
            })
            .ok_or_else(|| ExchangeError::Other("/v5/market/time: no timeNano".into()))?;
        let offset = server - (t0 + t1) / 2;
        self.time_offset_ms.store(offset, Ordering::Relaxed);
        Ok(offset)
    }

    /// Bybit's answer to a timestamp outside its window (`10002`), the one
    /// error a clock re-sync can fix. Python reaches the same verdict by
    /// type: ccxt maps `10002` to `InvalidNonce`, which
    /// `_is_exchange_time_sync_error` matches (passivbot.py:2527-2578).
    /// Matching the code directly is the equivalent here; the text needles
    /// only cover a differently-worded body.
    fn is_timestamp_error(err: &ExchangeError) -> bool {
        match err {
            ExchangeError::Rejected { code, msg } => {
                let m = msg.to_ascii_lowercase();
                code == "10002"
                    || m.contains("recv_window")
                    || m.contains("recvwindow")
                    || m.contains("server timestamp")
                    || m.contains("timestamp for this request")
            }
            _ => false,
        }
    }

    /// One signed attempt, then — only for a timestamp error — a clock
    /// re-sync and a single retry (`_maybe_recover_exchange_time_sync`).
    /// Without it a drifting host clock turns every cycle into an error and
    /// the runner restarts itself out of its daily budget.
    async fn signed_with_time_resync<F, Fut>(&self, attempt: F) -> Result<Value, ExchangeError>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<Value, ExchangeError>>,
    {
        match attempt().await {
            Err(e) if Self::is_timestamp_error(&e) => {
                let before = self.time_offset_ms();
                match self.sync_time().await {
                    Ok(after) => tracing::warn!(
                        before_ms = before,
                        after_ms = after,
                        "[time] exchange rejected the request timestamp; clock re-synced, retrying"
                    ),
                    Err(sync_err) => {
                        tracing::warn!(error = %sync_err, "[time] clock re-sync failed");
                        return Err(e);
                    }
                }
                attempt().await
            }
            other => other,
        }
    }

    pub async fn private_get(
        &self,
        path: &str,
        params: &[(&str, String)],
    ) -> Result<Value, ExchangeError> {
        self.signed_with_time_resync(|| self.private_get_once(path, params))
            .await
    }

    async fn private_get_once(
        &self,
        path: &str,
        params: &[(&str, String)],
    ) -> Result<Value, ExchangeError> {
        let qs = query_string(params);
        let ts = self.now_ms();
        let sig = sign::signature(
            &self.cfg.secret,
            ts,
            &self.cfg.api_key,
            self.cfg.recv_window_ms,
            &qs,
        );
        let url = if qs.is_empty() {
            format!("{}{}", self.cfg.base_url, path)
        } else {
            format!("{}{}?{}", self.cfg.base_url, path, qs)
        };
        let req = self.http.get(url).headers(self.auth_headers(ts, &sig));
        self.send(req).await
    }

    pub async fn private_post(&self, path: &str, body: &Value) -> Result<Value, ExchangeError> {
        self.signed_with_time_resync(|| self.private_post_once(path, body))
            .await
    }

    async fn private_post_once(&self, path: &str, body: &Value) -> Result<Value, ExchangeError> {
        let raw = serde_json::to_string(body).map_err(|e| ExchangeError::Other(e.to_string()))?;
        let ts = self.now_ms();
        let sig = sign::signature(
            &self.cfg.secret,
            ts,
            &self.cfg.api_key,
            self.cfg.recv_window_ms,
            &raw,
        );
        let req = self
            .http
            .post(format!("{}{}", self.cfg.base_url, path))
            .headers(self.auth_headers(ts, &sig))
            .header("Content-Type", "application/json")
            .body(raw);
        // POST only, exactly as ccxt does it (bybit.py:9424-9427: the broker
        // id becomes `Referer` on POST and on nothing else). ccxt omits the
        // header when `brokerId` is unset, so an empty string omits it here.
        let req = if self.cfg.broker_id.is_empty() {
            req
        } else {
            req.header("Referer", self.cfg.broker_id.clone())
        };
        self.send(req).await
    }

    fn auth_headers(&self, ts: u64, sig: &str) -> reqwest::header::HeaderMap {
        use reqwest::header::{HeaderMap, HeaderValue};
        let mut h = HeaderMap::new();
        let put = |h: &mut HeaderMap, k: &'static str, v: String| {
            if let Ok(val) = HeaderValue::from_str(&v) {
                h.insert(k, val);
            }
        };
        put(&mut h, "X-BAPI-API-KEY", self.cfg.api_key.clone());
        put(&mut h, "X-BAPI-TIMESTAMP", ts.to_string());
        put(
            &mut h,
            "X-BAPI-RECV-WINDOW",
            self.cfg.recv_window_ms.to_string(),
        );
        put(&mut h, "X-BAPI-SIGN", sig.to_string());
        put(&mut h, "X-BAPI-SIGN-TYPE", "2".to_string());
        h
    }

    /// Walk `nextPageCursor` pages (Python: `_do_fetch_positions_paginated`,
    /// `_do_fetch_open_orders`).
    async fn paginate(
        &self,
        path: &str,
        base: &[(&str, String)],
        limit: usize,
    ) -> Result<Vec<Value>, ExchangeError> {
        let mut pages = Vec::new();
        let mut cursor: Option<String> = None;
        loop {
            let mut params: Vec<(&str, String)> = base.to_vec();
            params.push(("limit", limit.to_string()));
            if let Some(c) = &cursor {
                params.push(("cursor", c.clone()));
            }
            let result = self.private_get(path, &params).await?;
            let n = result
                .get("list")
                .and_then(Value::as_array)
                .map_or(0, Vec::len);
            let next = parse::next_cursor(&result);
            pages.push(result);
            match next {
                Some(c) if n >= limit => cursor = Some(c),
                _ => break,
            }
        }
        Ok(pages)
    }

    /// `POST /v5/order/create` body as ccxt `create_order_request` builds it
    /// from Python's `execute_order` (passivbot.py:22351-22367:
    /// `type=order["type"]`, `price=order["price"]`, params from
    /// `exchanges/bybit.py::_build_order_params`: positionIdx, timeInForce,
    /// orderLinkId). ccxt sends `price` only for limit orders and refuses
    /// `postOnly` on market orders (`handle_post_only`), so a market order
    /// goes out as `orderType: Market`, `timeInForce: GTC`, no price.
    ///
    /// No `reduceOnly`, ever. `_build_order_params` returns exactly three
    /// keys and that is not one of them, and ccxt only sets `reduceOnly`
    /// itself on the trigger-order branch, so the Python bot's Bybit orders
    /// -- closes included -- carry no such field. Closing is expressed by
    /// `positionIdx` alone: in hedge mode a Sell on positionIdx 1 can only
    /// reduce the long. We used to send `reduceOnly: <bool>`, believing it
    /// matched (D11's inventory says so, wrongly); D24 has the account where
    /// the two runtimes placed the same 2.25 USDT entry seconds apart and
    /// only ours came back `110094 Order does not meet minimum order value`.
    fn order_body(&self, o: &NewOrder) -> Result<Value, ExchangeError> {
        let m = self.market(&o.symbol)?;
        let market = o.is_market();
        let mut body = json!({
            "category": "linear",
            "symbol": m.id,
            "side": match o.side { Side::Buy => "Buy", Side::Sell => "Sell" },
            "orderType": if market { "Market" } else { "Limit" },
            "qty": parse::fmt_step(o.qty.abs(), m.qty_step),
            "timeInForce": if o.post_only && !market { "PostOnly" } else { "GTC" },
            "positionIdx": match o.pside { PositionSide::Long => 1, PositionSide::Short => 2 },
            "orderLinkId": o.client_id,
        });
        if !market {
            body["price"] = Value::String(parse::fmt_step(o.price, m.price_step));
        }
        Ok(body)
    }

    async fn create_one(&self, o: &NewOrder) -> OrderResult<OpenOrder> {
        let body = self.order_body(o)?;
        let result = self.private_post("/v5/order/create", &body).await?;
        let id = result
            .get("orderId")
            .and_then(Value::as_str)
            .ok_or_else(|| ExchangeError::Malformed("order/create without orderId".into()))?;
        Ok(OpenOrder {
            id: id.to_string(),
            client_id: Some(o.client_id.clone()),
            symbol: o.symbol.clone(),
            side: o.side,
            pside: o.pside,
            qty: o.qty.abs(),
            price: o.price,
            reduce_only: o.reduce_only,
            created_ms: Some(self.now_ms()),
        })
    }

    /// `execute_cancellation` (passivbot.py:22373-22408): an "already gone"
    /// error is a success carrying the ambiguity marker.
    async fn cancel_one(&self, id: &str, symbol: &str) -> OrderResult<CancelAck> {
        let body = json!({"category": "linear", "symbol": self.id_of(symbol)?, "orderId": id});
        match self.private_post("/v5/order/cancel", &body).await {
            Ok(_) => Ok(CancelAck {
                id: id.to_string(),
                already_gone: false,
            }),
            Err(e) if is_already_gone(&e) => {
                tracing::info!(order_id = id, %symbol, "[order] cancel skipped: order already gone ({e})");
                Ok(CancelAck {
                    id: id.to_string(),
                    already_gone: true,
                })
            }
            Err(e) => Err(e),
        }
    }
}

#[async_trait]
impl ExchangeClient for BybitClient {
    async fn sync_time(&self) -> Result<i64, ExchangeError> {
        BybitClient::sync_time(self).await
    }

    async fn load_markets(&self) -> Result<Vec<MarketSpec>, ExchangeError> {
        let mut all = Vec::new();
        let mut cursor: Option<String> = None;
        loop {
            let mut params = vec![
                ("category", "linear".to_string()),
                ("limit", "1000".to_string()),
            ];
            if let Some(c) = &cursor {
                params.push(("cursor", c.clone()));
            }
            let result = self
                .public_get("/v5/market/instruments-info", &params)
                .await?;
            all.extend(parse::parse_markets(
                &result,
                self.cfg.maker_fee,
                self.cfg.taker_fee,
            )?);
            match parse::next_cursor(&result) {
                Some(c) => cursor = Some(c),
                None => break,
            }
        }
        if let (Ok(mut ids), Ok(mut markets)) = (self.ids.write(), self.markets.write()) {
            ids.clear();
            markets.clear();
            for m in &all {
                ids.insert(m.id.clone(), m.symbol.clone());
                markets.insert(m.symbol.clone(), m.clone());
            }
        }
        Ok(all)
    }

    async fn fetch_balance(&self) -> Result<Balance, ExchangeError> {
        let result = self
            .private_get(
                "/v5/account/wallet-balance",
                &[("accountType", "UNIFIED".to_string())],
            )
            .await?;
        parse::parse_balance(&result)
    }

    async fn fetch_positions(&self) -> Result<Vec<Position>, ExchangeError> {
        let base = [
            ("category", "linear".to_string()),
            ("settleCoin", parse::QUOTE.to_string()),
        ];
        let mut out = Vec::new();
        for page in self.paginate("/v5/position/list", &base, 200).await? {
            out.extend(parse::parse_positions(&page, &|id| self.symbol_of(id))?);
        }
        // Python keys by symbol+side and keeps the first occurrence.
        let mut seen = std::collections::HashSet::new();
        out.retain(|p| seen.insert((p.symbol.clone(), p.pside)));
        Ok(out)
    }

    async fn fetch_open_orders(&self) -> Result<Vec<OpenOrder>, ExchangeError> {
        let base = [
            ("category", "linear".to_string()),
            ("settleCoin", parse::QUOTE.to_string()),
        ];
        let mut out = Vec::new();
        for page in self.paginate("/v5/order/realtime", &base, 50).await? {
            out.extend(parse::parse_open_orders(&page, &|id| self.symbol_of(id))?);
        }
        let mut seen = std::collections::HashSet::new();
        out.retain(|o| seen.insert(o.id.clone()));
        out.sort_by_key(|o| o.created_ms.unwrap_or(0));
        Ok(out)
    }

    async fn fetch_tickers(&self) -> Result<Vec<Ticker>, ExchangeError> {
        let result = self
            .public_get("/v5/market/tickers", &[("category", "linear".to_string())])
            .await?;
        parse::parse_tickers(&result, &|id| self.symbol_of(id))
    }

    async fn fetch_ohlcv(
        &self,
        symbol: &str,
        timeframe: &str,
        since_ms: Option<u64>,
        limit: usize,
    ) -> Result<Vec<Candle>, ExchangeError> {
        let id = self.id_of(symbol)?;
        let (interval, period_ms) = match timeframe {
            "1m" => ("1", 60_000u64),
            "1h" => ("60", 3_600_000u64),
            other => return Err(ExchangeError::NotSupported(format!("timeframe {other}"))),
        };
        let limit = if limit == 0 { 1000 } else { limit.min(1000) };
        let base = |start: Option<u64>| {
            let mut p = vec![
                ("category", "linear".to_string()),
                ("symbol", id.clone()),
                ("interval", interval.to_string()),
                ("limit", limit.to_string()),
            ];
            if let Some(s) = start {
                p.push(("start", s.to_string()));
            }
            p
        };
        let Some(since) = since_ms else {
            let result = self.public_get("/v5/market/kline", &base(None)).await?;
            return parse::parse_klines(&result);
        };
        let mut since = since / period_ms * period_ms;
        let mut all: std::collections::BTreeMap<u64, Candle> = std::collections::BTreeMap::new();
        for _ in 0..5 {
            let result = self
                .public_get("/v5/market/kline", &base(Some(since)))
                .await?;
            let page = parse::parse_klines(&result)?;
            if page.is_empty() {
                break;
            }
            let n = page.len();
            for c in page {
                all.insert(c[0] as u64, c);
            }
            if n < limit {
                break;
            }
            since = all.keys().next_back().copied().unwrap_or(since);
        }
        Ok(all.into_values().collect())
    }

    /// `exchanges/bybit.py::fetch_fills` (279-308): without `start_time` a
    /// single `fetch_my_trades()` (Bybit's default window); with it, the
    /// range `[start, end or now + 4h]` is walked backwards in 7-day
    /// windows (ccxt `paginate` = `nextPageCursor` inside each window),
    /// deduplicated by execution id (`fetch_pnls` joins on `execId`).
    async fn fetch_fills(
        &self,
        symbol: Option<&str>,
        start_ms: Option<u64>,
        end_ms: Option<u64>,
    ) -> Result<Vec<Fill>, ExchangeError> {
        let mut base = vec![
            ("category", "linear".to_string()),
            ("execType", "Trade".to_string()),
        ];
        if let Some(s) = symbol {
            base.push(("symbol", self.id_of(s)?));
        }
        let windows: Vec<(Option<u64>, Option<u64>)> = match start_ms {
            None => vec![(None, end_ms)],
            Some(start) => {
                let end = end_ms.unwrap_or_else(|| self.now_ms() + FILLS_END_SLACK_MS);
                weekly_windows(start, end, FILLS_MAX_WINDOWS)
                    .into_iter()
                    .map(|(s, e)| (Some(s), Some(e)))
                    .collect()
            }
        };
        let mut out = Vec::new();
        for (ws, we) in windows {
            let mut params = base.clone();
            if let Some(s) = ws {
                params.push(("startTime", s.to_string()));
            }
            if let Some(e) = we {
                params.push(("endTime", e.to_string()));
            }
            let mut n = 0usize;
            for page in self.paginate("/v5/execution/list", &params, 100).await? {
                let fills = parse::parse_fills(&page, &|id| self.symbol_of(id))?;
                n += fills.len();
                out.extend(fills);
            }
            if let (Some(s), Some(e)) = (ws, we) {
                tracing::debug!(start = s, end = e, fills = n, "fetched fills window");
            }
        }
        let mut seen = std::collections::HashSet::new();
        out.retain(|f| seen.insert(f.id.clone()));
        out.sort_by_key(|f| (f.timestamp_ms, f.id.clone()));
        Ok(out)
    }

    /// `exchanges/bybit.py::fetch_pnls_sub` (200-225): without `start_time`
    /// one `fetch_pnl` page; with it, explicit 7-day windows from
    /// `end or now + 1d` back to `start` (at most 52), each cursor-paginated
    /// (`fetch_pnl`, 227-277), deduplicated by `(orderId, updatedTime)`.
    async fn fetch_closed_pnl(
        &self,
        start_ms: Option<u64>,
        end_ms: Option<u64>,
    ) -> Result<Vec<ClosedPnl>, ExchangeError> {
        let base = vec![("category", "linear".to_string())];
        let windows: Vec<(Option<u64>, Option<u64>)> = match start_ms {
            None => vec![(None, end_ms)],
            Some(start) => {
                let end = end_ms.unwrap_or_else(|| self.now_ms() + PNL_END_SLACK_MS);
                weekly_windows(start, end, PNL_MAX_WINDOWS)
                    .into_iter()
                    .map(|(s, e)| (Some(s), Some(e)))
                    .collect()
            }
        };
        let mut out = Vec::new();
        for (ws, we) in windows {
            let mut params = base.clone();
            if let Some(s) = ws {
                params.push(("startTime", s.to_string()));
            }
            if let Some(e) = we {
                params.push(("endTime", e.to_string()));
            }
            for page in self
                .paginate("/v5/position/closed-pnl", &params, 100)
                .await?
            {
                out.extend(parse::parse_closed_pnl(&page, &|id| self.symbol_of(id))?);
            }
        }
        let mut seen = std::collections::HashSet::new();
        out.retain(|p| seen.insert((p.order_id.clone(), p.timestamp_ms)));
        out.sort_by_key(|p| (p.timestamp_ms, p.order_id.clone()));
        Ok(out)
    }

    async fn create_orders(&self, orders: &[NewOrder]) -> Vec<OrderResult<OpenOrder>> {
        futures::future::join_all(orders.iter().map(|o| self.create_one(o))).await
    }

    async fn cancel_orders(&self, orders: &[(String, String)]) -> Vec<OrderResult<CancelAck>> {
        futures::future::join_all(
            orders
                .iter()
                .map(|(id, symbol)| self.cancel_one(id, symbol)),
        )
        .await
    }

    /// `exchanges/bybit.py::update_exchange_config` (584-596): ccxt
    /// `set_position_mode(True)` = `POST /v5/position/switch-mode`
    /// `{category: linear, coin: USDT, mode: 3}` (ccxt bybit.py:6801-6836);
    /// `110025` / "not modified" means already in hedge mode.
    async fn set_hedge_mode(&self) -> Result<(), ExchangeError> {
        let body = json!({"category": "linear", "coin": parse::QUOTE, "mode": 3});
        match self.private_post("/v5/position/switch-mode", &body).await {
            Ok(_) => Ok(()),
            Err(e) if is_not_modified(&e) => {
                tracing::debug!("[config] hedge mode already set (not modified)");
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    /// `exchanges/bybit.py::update_exchange_config_by_symbols` (553-582):
    /// `cca.set_margin_mode(margin_mode, symbol, {"leverage": leverage})`
    /// tolerating `110026` / "not modified", then
    /// `cca.set_leverage(leverage, symbol)` tolerating `110043` / "not
    /// modified"; anything else raises and the symbol is retried later with
    /// backoff (passivbot.py:10149-10240).
    ///
    /// ccxt `set_margin_mode` (bybit.py:6677-6760) branches on
    /// `is_unified_enabled()`: a unified account gets the account-wide
    /// `POST /v5/account/set-margin-mode {setMarginMode: REGULAR_MARGIN |
    /// ISOLATED_MARGIN}` (the `leverage` param is carried along by
    /// `self.extend(request, params)`); a classic account gets the
    /// per-symbol `POST /v5/position/switch-isolated`. `set_leverage`
    /// (6762-6799) is `POST /v5/position/set-leverage` with
    /// `buyLeverage = sellLeverage = number_to_string(leverage)`. The
    /// `leverage` carried into `set_margin_mode` stays a JSON number, because
    /// nothing stringifies it on that path.
    async fn configure_symbol(
        &self,
        symbol: &str,
        leverage: f64,
        margin_mode: MarginMode,
    ) -> Result<(), ExchangeError> {
        let id = self.id_of(symbol)?;
        // `set_leverage` stringifies (ccxt `number_to_string`); the leverage
        // that rides along on `set_margin_mode` does not -- it is whatever
        // passivbot passed in `params`, and `_calc_leverage_for_symbol`
        // returns an int. Two spellings of the same number, and the parity
        // test holds us to both.
        let lev_int = leverage as i64;
        let lev = format!("{lev_int}");
        let unified = self.is_unified_account().await?;
        let (path, mode_body) = if unified {
            (
                "/v5/account/set-margin-mode",
                json!({
                    "setMarginMode": match margin_mode {
                        MarginMode::Cross => "REGULAR_MARGIN",
                        MarginMode::Isolated => "ISOLATED_MARGIN",
                    },
                    "leverage": lev_int,
                }),
            )
        } else {
            (
                "/v5/position/switch-isolated",
                json!({
                    "category": "linear",
                    "symbol": id,
                    "tradeMode": match margin_mode { MarginMode::Cross => 0, MarginMode::Isolated => 1 },
                    "buyLeverage": lev,
                    "sellLeverage": lev,
                }),
            )
        };
        match self.private_post(path, &mode_body).await {
            Ok(_) => {}
            Err(e) if is_not_modified(&e) => {
                tracing::debug!(%symbol, "margin mode already set (not modified)");
            }
            Err(e) => return Err(e),
        }
        let lev_body =
            json!({"category": "linear", "symbol": id, "buyLeverage": lev, "sellLeverage": lev});
        match self
            .private_post("/v5/position/set-leverage", &lev_body)
            .await
        {
            Ok(_) => Ok(()),
            Err(e) if is_not_modified(&e) => {
                tracing::debug!(%symbol, "leverage already set (not modified)");
                Ok(())
            }
            Err(e) => Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn envelope_ok_and_errors() {
        let ok = json!({"retCode": 0, "retMsg": "OK", "result": {"list": []}});
        assert_eq!(
            BybitClient::unwrap_envelope(ok).unwrap(),
            json!({"list": []})
        );
        let rl =
            BybitClient::unwrap_envelope(json!({"retCode": 10006, "retMsg": "Too many visits"}))
                .unwrap_err();
        assert!(matches!(rl, ExchangeError::RateLimited { .. }));
        let auth = BybitClient::unwrap_envelope(
            json!({"retCode": 10003, "retMsg": "API key is invalid."}),
        )
        .unwrap_err();
        assert!(matches!(auth, ExchangeError::Auth(_)));
        let rej = BybitClient::unwrap_envelope(
            json!({"retCode": 110007, "retMsg": "ab not enough for new order"}),
        )
        .unwrap_err();
        assert_eq!(rej.code(), Some("110007"));
    }

    #[test]
    fn already_gone_and_not_modified_rules() {
        let gone = ExchangeError::Rejected {
            code: "110001".into(),
            msg: "order not exists or too late to cancel".into(),
        };
        assert!(is_already_gone(&gone));
        let gone2 = ExchangeError::Rejected {
            code: "170213".into(),
            msg: "Order does not exist.".into(),
        };
        assert!(is_already_gone(&gone2));
        let other = ExchangeError::Rejected {
            code: "110007".into(),
            msg: "ab not enough".into(),
        };
        assert!(!is_already_gone(&other));
        for c in ["110025", "110026", "110043"] {
            assert!(is_not_modified(&ExchangeError::Rejected {
                code: c.into(),
                msg: "x".into()
            }));
        }
        assert!(is_not_modified(&ExchangeError::Rejected {
            code: "1".into(),
            msg: "leverage not modified".into()
        }));
    }

    #[test]
    fn timestamp_errors_are_the_only_resync_trigger() {
        // The real 10002 body, which carries `recv_window` with an
        // underscore -- the code, not the wording, is what identifies it.
        let ts = ExchangeError::Rejected {
            code: "10002".into(),
            msg: "invalid request, please check your server timestamp or                   recv_window param. req_timestamp[1788800000000]                   server_timestamp[1788799998000] recv_window[5000]"
                .into(),
        };
        assert!(BybitClient::is_timestamp_error(&ts));
        // A body worded differently but still about the window.
        assert!(BybitClient::is_timestamp_error(&ExchangeError::Rejected {
            code: "1".into(),
            msg: "Timestamp for this request is outside of the recvWindow.".into(),
        }));
        // Everything else must NOT cost a re-sync + retry: a rejected order
        // would be sent twice.
        assert!(!BybitClient::is_timestamp_error(&ExchangeError::Rejected {
            code: "110007".into(),
            msg: "ab not enough for new order".into(),
        }));
        assert!(!BybitClient::is_timestamp_error(&ExchangeError::Auth(
            "10010 Unmatched IP".into()
        )));
        assert!(!BybitClient::is_timestamp_error(&ExchangeError::Network(
            "connection reset".into()
        )));
    }

    #[test]
    fn signed_timestamps_carry_the_synced_offset() {
        let client = BybitClient::new(BybitConfig::mainnet("k", "s")).unwrap();
        assert_eq!(client.time_offset_ms(), 0);
        let before = client.now_ms();
        client.time_offset_ms.store(-1_500, Ordering::Relaxed);
        assert_eq!(client.time_offset_ms(), -1_500);
        // A host clock 1.5 s fast is exactly what Bybit rejects; the signed
        // timestamp must come back inside the window, not track the host.
        let after = client.now_ms();
        assert!(
            after + 1_400 <= before + 50,
            "expected the offset to move the timestamp back: {before} -> {after}"
        );
    }

    #[test]
    fn order_body_shape() {
        let client = BybitClient::new(BybitConfig::mainnet("k", "s")).unwrap();
        client.markets.write().unwrap().insert(
            "ETH/USDT:USDT".into(),
            MarketSpec {
                symbol: "ETH/USDT:USDT".into(),
                id: "ETHUSDT".into(),
                qty_step: 0.01,
                price_step: 0.01,
                min_qty: 0.01,
                min_cost: 0.1,
                min_notional: Some(5.0),
                contract_size: 1.0,
                max_leverage: 100.0,
                maker_fee: 0.0002,
                taker_fee: 0.00055,
                active: true,
            },
        );
        let o = NewOrder {
            client_id: "x-pb-abc".into(),
            symbol: "ETH/USDT:USDT".into(),
            side: Side::Buy,
            pside: PositionSide::Long,
            qty: 0.05,
            price: 3814.26,
            reduce_only: false,
            post_only: true,
            order_type: crate::OrderType::Limit,
        };
        let body = client.order_body(&o).unwrap();
        assert_eq!(
            body,
            json!({"category":"linear","symbol":"ETHUSDT","side":"Buy","orderType":"Limit","qty":"0.05","price":"3814.26","timeInForce":"PostOnly","positionIdx":1,"orderLinkId":"x-pb-abc"})
        );
        let sell = NewOrder {
            side: Side::Sell,
            pside: PositionSide::Long,
            reduce_only: true,
            post_only: false,
            ..o
        };
        let body = client.order_body(&sell).unwrap();
        assert_eq!(body["timeInForce"], "GTC");
        assert_eq!(body["side"], "Sell");
        // A close is a Sell on the long's positionIdx and nothing else; the
        // Python bot never sends `reduceOnly` to Bybit, so neither do we.
        assert!(body.get("reduceOnly").is_none());
        // Market order (engine `execution_type = market`): ccxt sends
        // `orderType: Market` without `price`, and never `PostOnly`.
        let market = NewOrder {
            side: Side::Sell,
            reduce_only: true,
            post_only: true,
            order_type: crate::OrderType::Market,
            ..sell
        };
        let body = client.order_body(&market).unwrap();
        assert_eq!(
            body,
            json!({"category":"linear","symbol":"ETHUSDT","side":"Sell","orderType":"Market","qty":"0.05","timeInForce":"GTC","positionIdx":1,"orderLinkId":"x-pb-abc"})
        );
        assert!(body.get("price").is_none());
    }

    #[test]
    fn weekly_windows_walk_the_whole_range_backwards() {
        let day = 24 * 60 * 60 * 1000u64;
        let end = 100 * day;
        // 30 days -> 5 windows of 7 days newest first, the last one clamped.
        let w = weekly_windows(end - 30 * day, end, FILLS_MAX_WINDOWS);
        assert_eq!(w.len(), 5);
        assert_eq!(w[0], (end - 7 * day, end));
        assert_eq!(w[1], (end - 14 * day, end - 7 * day));
        assert_eq!(w[4], (end - 30 * day, end - 28 * day));
        // Contiguous coverage of exactly [start, end].
        for pair in w.windows(2) {
            assert_eq!(pair[0].0, pair[1].1);
        }
        // Less than a week: one clamped window.
        assert_eq!(
            weekly_windows(end - 3 * day, end, FILLS_MAX_WINDOWS),
            vec![(end - 3 * day, end)]
        );
        // Exactly a week: one window, no zero-length tail.
        assert_eq!(
            weekly_windows(end - 7 * day, end, FILLS_MAX_WINDOWS),
            vec![(end - 7 * day, end)]
        );
        // Degenerate ranges produce nothing; the cap bounds the walk.
        assert!(weekly_windows(end, end, FILLS_MAX_WINDOWS).is_empty());
        assert_eq!(weekly_windows(0, end, 3).len(), 3);
        // fetch_pnls_sub arithmetic: sts = end - week*i, ets = sts + week, sts = max(sts, start).
        let start = end - 20 * day;
        let mut expected = Vec::new();
        for i in 1..PNL_MAX_WINDOWS as u64 {
            let sts = end - 7 * day * i;
            let ets = sts + 7 * day;
            let sts = sts.max(start);
            expected.push((sts, ets));
            if sts <= start {
                break;
            }
        }
        assert_eq!(weekly_windows(start, end, PNL_MAX_WINDOWS), expected);
    }

    #[test]
    fn query_string_order_is_preserved() {
        assert_eq!(
            query_string(&[("category", "linear".into()), ("settleCoin", "USDT".into())]),
            "category=linear&settleCoin=USDT"
        );
    }
}
