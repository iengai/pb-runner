//! Every request this client sends, against the one ccxt would have sent.
//!
//! D11 replaced the ccxt Rust port with a hand-written client and wrote down
//! the check meant to keep that honest -- "record the Python bot's ccxt
//! requests/responses ... and compare against this client's requests". It was
//! never built. D24 is what it would have caught: a `reduceOnly` field on
//! every order that passivbot has never sent on any, which on a small account
//! turned every entry into `110094 Order does not meet minimum order value`
//! while the Python bot, same key and same size, was filled.
//!
//! The recorded side is `tests/fixtures/ccxt_requests.json`, produced by
//! `tools/ccxt_request_fixtures.py` from the pinned ccxt driven exactly as
//! passivbot v8.1.0 drives it. This side points the client at a local socket
//! that answers every endpoint with a canned envelope, and compares what it
//! put on the wire.
//!
//! The four existing harnesses (diffcheck, snapcheck, plancheck, mockrun)
//! replay recorded inputs and compare PLANS. A request body is not a plan.
//! This file is the only thing in the repo that looks at the bytes.
//!
//! Divergences that are deliberate are listed in `CASES` with a reason, and
//! nothing else is tolerated -- a new one fails here.

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};

use pb_exchange_bybit::bybit::{BybitClient, BybitConfig};
use pb_exchange_bybit::{ExchangeClient, MarginMode, NewOrder, OrderType, PositionSide, Side};
use serde_json::{json, Value};

const FIXTURE: &str = include_str!("../../../tests/fixtures/ccxt_requests.json");

// ---------------------------------------------------------------------------
// A Bybit that only remembers what it was asked
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
struct Recorded {
    method: String,
    path: String,
    query: BTreeMap<String, String>,
    body: Option<Value>,
    /// Lowercased header names, as they reached the socket.
    headers: BTreeMap<String, String>,
}

/// Enough of an instruments-info response for `load_markets` to build the two
/// symbols the cases use. Fields are the ones `parse_markets` reads.
fn instruments_info() -> Value {
    let sym = |base: &str, qty_step: &str, min_qty: &str, tick: &str| {
        json!({
            "symbol": format!("{base}USDT"),
            "baseCoin": base,
            "quoteCoin": "USDT",
            "settleCoin": "USDT",
            "contractType": "LinearPerpetual",
            "status": "Trading",
            "lotSizeFilter": {
                "qtyStep": qty_step, "minOrderQty": min_qty,
                "maxOrderQty": "7500000", "minNotionalValue": "5"
            },
            "priceFilter": {"tickSize": tick},
            "leverageFilter": {"minLeverage": "1", "maxLeverage": "100"}
        })
    };
    json!({"list": [
        sym("XRP", "0.1", "0.1", "0.0001"),
        sym("ETH", "0.01", "0.01", "0.01"),
    ], "nextPageCursor": ""})
}

fn canned(path: &str) -> Value {
    let result = match path {
        "/v5/market/instruments-info" => instruments_info(),
        "/v5/user/query-api" => json!({"unified": 1, "uta": 1}),
        // Enough to terminate every pagination loop and every parser.
        _ => json!({"list": [], "nextPageCursor": "", "category": "linear"}),
    };
    json!({"retCode": 0, "retMsg": "OK", "result": result})
}

struct MockBybit {
    base_url: String,
    seen: Arc<Mutex<Vec<Recorded>>>,
}

impl MockBybit {
    fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let seen = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&seen);
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { break };
                if let Some(rec) = serve_one(stream) {
                    sink.lock().unwrap().push(rec);
                }
            }
        });
        Self { base_url, seen }
    }

    /// The requests since the last call, oldest first.
    fn drain(&self) -> Vec<Recorded> {
        std::mem::take(&mut *self.seen.lock().unwrap())
    }
}

/// One request, one response, connection closed. Blocking on purpose: the
/// client's runtime is elsewhere and this thread has nothing else to do.
fn serve_one(mut stream: TcpStream) -> Option<Recorded> {
    let mut reader = BufReader::new(stream.try_clone().ok()?);
    let mut request_line = String::new();
    reader.read_line(&mut request_line).ok()?;
    let mut parts = request_line.split_whitespace();
    let method = parts.next()?.to_string();
    let target = parts.next()?.to_string();

    let mut content_length = 0usize;
    let mut headers: BTreeMap<String, String> = BTreeMap::new();
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).ok()? == 0 {
            break;
        }
        let line = line.trim_end();
        if line.is_empty() {
            break;
        }
        if let Some((k, v)) = line.split_once(':') {
            let (k, v) = (k.trim().to_ascii_lowercase(), v.trim().to_string());
            if k == "content-length" {
                content_length = v.parse().unwrap_or(0);
            }
            headers.insert(k, v);
        }
    }
    let body = if content_length > 0 {
        let mut buf = vec![0u8; content_length];
        reader.read_exact(&mut buf).ok()?;
        serde_json::from_slice::<Value>(&buf).ok()
    } else {
        None
    };

    let (path, query_str) = match target.split_once('?') {
        Some((p, q)) => (p.to_string(), q.to_string()),
        None => (target.clone(), String::new()),
    };
    let query = query_str
        .split('&')
        .filter(|s| !s.is_empty())
        .filter_map(|kv| {
            kv.split_once('=')
                .map(|(k, v)| (k.to_string(), v.to_string()))
        })
        .collect::<BTreeMap<_, _>>();

    let payload = canned(&path).to_string();
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        payload.len(),
        payload
    );
    let _ = stream.write_all(response.as_bytes());
    let _ = stream.flush();

    Some(Recorded {
        method,
        path,
        headers,
        query,
        body,
    })
}

// ---------------------------------------------------------------------------
// What to compare
// ---------------------------------------------------------------------------

/// One passivbot call site: the label recorded from ccxt, and the deliberate
/// differences, each with the reason it is allowed to stand.
struct Case {
    ccxt: &'static str,
    allowed: &'static [(&'static str, &'static str)],
}

const CASES: &[(&str, Case)] = &[
    (
        "create_order.entry_limit_gtc",
        Case {
            ccxt: "create_order.entry_limit_gtc",
            allowed: &[],
        },
    ),
    (
        "create_order.entry_limit_post_only",
        Case {
            ccxt: "create_order.entry_limit_post_only",
            allowed: &[],
        },
    ),
    (
        "create_order.close_limit_gtc",
        Case {
            ccxt: "create_order.close_limit_gtc",
            allowed: &[],
        },
    ),
    (
        "create_order.close_market",
        Case {
            ccxt: "create_order.close_market",
            allowed: &[],
        },
    ),
    (
        "cancel_order",
        Case {
            ccxt: "cancel_order",
            allowed: &[],
        },
    ),
    (
        "set_position_mode",
        Case {
            ccxt: "set_position_mode",
            allowed: &[],
        },
    ),
    (
        "set_leverage",
        Case {
            ccxt: "set_leverage",
            allowed: &[],
        },
    ),
    (
        "set_margin_mode",
        Case {
            ccxt: "set_margin_mode",
            allowed: &[],
        },
    ),
    (
        "fetch_balance",
        Case {
            ccxt: "fetch_balance",
            allowed: &[],
        },
    ),
    (
        "fetch_positions",
        Case {
            ccxt: "fetch_positions",
            allowed: &[],
        },
    ),
    (
        "fetch_open_orders",
        Case {
            ccxt: "fetch_open_orders",
            allowed: &[],
        },
    ),
    (
        "fetch_tickers",
        Case {
            ccxt: "fetch_tickers",
            allowed: &[],
        },
    ),
    (
        "fetch_ohlcv",
        Case {
            ccxt: "fetch_ohlcv",
            allowed: &[(
                "end",
                "not sent: ccxt's fetch_ohlcv takes only `since` and `limit`, \
                 and neither does the Python bot",
            )],
        },
    ),
    (
        "fetch_my_trades",
        Case {
            ccxt: "fetch_my_trades",
            // exchanges/bybit.py::fetch_fills walks backwards by endTime with
            // ccxt's own `paginate`; we walk explicit 7-day windows (the
            // window bounds are ours, the filter keys are ccxt's).
            allowed: &[
                (
                    "startTime",
                    "our explicit 7-day windows; ccxt paginates by endTime",
                ),
                ("endTime", "same"),
                ("limit", "ours is the page size ccxt's paginate uses (100)"),
            ],
        },
    ),
    (
        "closed_pnl",
        Case {
            ccxt: "closed_pnl",
            allowed: &[
                (
                    "startTime",
                    "explicit 7-day windows, as fetch_pnls_sub does",
                ),
                ("endTime", "same"),
                (
                    "limit",
                    "fetch_pnl passes limit=100; the fixture call does too",
                ),
            ],
        },
    ),
];

fn fixture_request(label: &str, path_hint: &str) -> Recorded {
    let doc: Value = serde_json::from_str(FIXTURE).expect("fixture parses");
    let reqs = doc["requests"].as_array().expect("requests array");
    let hit = reqs
        .iter()
        .find(|r| r["call"] == label && r["path"].as_str() == Some(path_hint))
        .unwrap_or_else(|| panic!("no ccxt request {label} on {path_hint} in the fixture"));
    Recorded {
        method: hit["method"].as_str().unwrap().to_string(),
        path: hit["path"].as_str().unwrap().to_string(),
        query: hit["query"]
            .as_object()
            .map(|m| {
                m.iter()
                    .map(|(k, v)| (k.clone(), v.as_str().unwrap_or_default().to_string()))
                    .collect()
            })
            .unwrap_or_default(),
        body: match &hit["body"] {
            Value::Null => None,
            v => Some(v.clone()),
        },
        headers: hit["headers"]
            .as_object()
            .map(|m| {
                m.iter()
                    .map(|(k, v)| {
                        (
                            k.to_ascii_lowercase(),
                            v.as_str().unwrap_or_default().to_string(),
                        )
                    })
                    .collect()
            })
            .unwrap_or_default(),
    }
}

/// Keys whose values differ, plus keys present on one side only.
fn diff(ours: &Recorded, theirs: &Recorded) -> Vec<String> {
    let flatten = |r: &Recorded| -> BTreeMap<String, String> {
        let mut m: BTreeMap<String, String> = r
            .query
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        if let Some(Value::Object(o)) = &r.body {
            for (k, v) in o {
                // Compared as text: `10` and `"10"` are different requests and
                // this harness exists to notice exactly that kind of thing.
                m.insert(k.clone(), v.to_string());
            }
        }
        m
    };
    let (a, b) = (flatten(ours), flatten(theirs));
    let mut out = Vec::new();
    for (k, v) in &a {
        match b.get(k) {
            None => out.push(format!("we send `{k}` = {v}, ccxt sends nothing")),
            Some(w) if w != v => out.push(format!("`{k}`: ours {v}, ccxt {w}")),
            _ => {}
        }
    }
    for (k, w) in &b {
        if !a.contains_key(k) {
            out.push(format!("ccxt sends `{k}` = {w}, we send nothing"));
        }
    }
    out
}

/// Every header ccxt sets deliberately, we must set with the same value.
/// Headers the HTTP stack adds on its own (host, accept, user-agent,
/// content-length) are not ccxt's and are not compared.
fn assert_headers(label: &str, ours: &Recorded, theirs: &Recorded) {
    for (name, want) in &theirs.headers {
        let key = name.to_ascii_lowercase();
        // Signature, clock and key: presence is the claim, the value is not
        // comparable between two clients.
        let volatile = matches!(
            key.as_str(),
            "x-bapi-sign" | "x-bapi-timestamp" | "x-bapi-api-key"
        );
        // ccxt puts a Content-Type on private GETs too, where there is no
        // body for it to describe and reqwest sends none.
        if key == "content-type" && ours.method == "GET" {
            continue;
        }
        let got = ours.headers.get(&key).unwrap_or_else(|| {
            panic!(
                "{label}: ccxt sends the header `{name}` to {} and we send none. On POST that includes `Referer`, which carries passivbot's broker code.",
                ours.path
            )
        });
        if !volatile {
            assert_eq!(got, want, "{label}: header `{name}` on {}", ours.path);
        }
    }
}

fn assert_matches(label: &str, ours: &Recorded, case: &Case) {
    let theirs = fixture_request(case.ccxt, &ours.path);
    assert_eq!(ours.method, theirs.method, "{label}: HTTP method");
    assert_eq!(ours.path, theirs.path, "{label}: path");
    assert_headers(label, ours, &theirs);
    let unexplained: Vec<String> = diff(ours, &theirs)
        .into_iter()
        .filter(|d| {
            !case
                .allowed
                .iter()
                .any(|(k, _)| d.contains(&format!("`{k}`")))
        })
        .collect();
    assert!(
        unexplained.is_empty(),
        "{label}: this client and the Python bot send different requests to {}:\n  {}\n\
         If a difference is deliberate, add it to CASES with the reason.",
        ours.path,
        unexplained.join("\n  ")
    );
}

// ---------------------------------------------------------------------------
// The run
// ---------------------------------------------------------------------------

/// The first request a call site actually put on the wire. The
/// unified-account probe is ccxt's too and is captured under its own call in
/// the fixture, so it is skipped where it precedes a write.
fn first_request(label: &str, recs: Vec<Recorded>) -> Recorded {
    recs.into_iter()
        .find(|r| r.path != "/v5/user/query-api")
        .unwrap_or_else(|| panic!("{label}: no request reached the exchange"))
}

fn order(client_id: &str, side: Side, price: f64, post_only: bool, market: bool) -> NewOrder {
    NewOrder {
        client_id: client_id.to_string(),
        symbol: "XRP/USDT:USDT".to_string(),
        side,
        pside: PositionSide::Long,
        qty: 1.6,
        price,
        reduce_only: side == Side::Sell,
        post_only,
        order_type: if market {
            OrderType::Market
        } else {
            OrderType::Limit
        },
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_request_matches_the_python_bots() {
    let mock = MockBybit::start();
    let mut cfg = BybitConfig::mainnet("test-key", "test-secret");
    cfg.base_url = mock.base_url.clone();
    let client = BybitClient::new(cfg).expect("client");

    client.load_markets().await.expect("markets");
    mock.drain();

    // Each entry drives one passivbot call site and keeps the request that
    // reached the socket. Results are ignored: canned envelopes make most
    // parsers unhappy, and the request is what is under test.
    let mut runs: Vec<(&str, Recorded)> = Vec::new();

    let _ = client
        .create_orders(&[order("0x0007pb-entry", Side::Buy, 1.4077, false, false)])
        .await;
    runs.push((
        "create_order.entry_limit_gtc",
        first_request("create_order.entry_limit_gtc", mock.drain()),
    ));

    let _ = client
        .create_orders(&[order("0x0007pb-postonly", Side::Buy, 1.4077, true, false)])
        .await;
    runs.push((
        "create_order.entry_limit_post_only",
        first_request("create_order.entry_limit_post_only", mock.drain()),
    ));

    let _ = client
        .create_orders(&[order("0x0007pb-close", Side::Sell, 1.5, false, false)])
        .await;
    runs.push((
        "create_order.close_limit_gtc",
        first_request("create_order.close_limit_gtc", mock.drain()),
    ));

    let _ = client
        .create_orders(&[order("0x0007pb-close", Side::Sell, 1.5, false, true)])
        .await;
    runs.push((
        "create_order.close_market",
        first_request("create_order.close_market", mock.drain()),
    ));

    let _ = client
        .cancel_orders(&[("1234567890".to_string(), "XRP/USDT:USDT".to_string())])
        .await;
    runs.push(("cancel_order", first_request("cancel_order", mock.drain())));

    let _ = client.set_hedge_mode().await;
    runs.push((
        "set_position_mode",
        first_request("set_position_mode", mock.drain()),
    ));

    // One call, two requests: margin mode then leverage (as passivbot does).
    let _ = client
        .configure_symbol("XRP/USDT:USDT", 10.0, MarginMode::Cross)
        .await;
    let config_reqs = mock.drain();
    let margin = config_reqs
        .iter()
        .find(|r| r.path == "/v5/account/set-margin-mode")
        .expect("set-margin-mode request")
        .clone();
    let leverage = config_reqs
        .iter()
        .find(|r| r.path == "/v5/position/set-leverage")
        .expect("set-leverage request")
        .clone();
    runs.push(("set_margin_mode", margin));
    runs.push(("set_leverage", leverage));

    let _ = client.fetch_balance().await;
    runs.push((
        "fetch_balance",
        first_request("fetch_balance", mock.drain()),
    ));

    let _ = client.fetch_positions().await;
    runs.push((
        "fetch_positions",
        first_request("fetch_positions", mock.drain()),
    ));

    let _ = client.fetch_open_orders().await;
    runs.push((
        "fetch_open_orders",
        first_request("fetch_open_orders", mock.drain()),
    ));

    let _ = client.fetch_tickers().await;
    runs.push((
        "fetch_tickers",
        first_request("fetch_tickers", mock.drain()),
    ));

    let _ = client
        .fetch_ohlcv("XRP/USDT:USDT", "1m", Some(1_788_000_000_000), 1000)
        .await;
    runs.push(("fetch_ohlcv", first_request("fetch_ohlcv", mock.drain())));

    let _ = client.fetch_fills(None, None, None).await;
    runs.push((
        "fetch_my_trades",
        first_request("fetch_my_trades", mock.drain()),
    ));

    let _ = client.fetch_closed_pnl(None, None).await;
    runs.push(("closed_pnl", first_request("closed_pnl", mock.drain())));

    assert_eq!(
        runs.len(),
        CASES.len(),
        "every case must have been driven exactly once"
    );
    for (label, recorded) in &runs {
        let case = CASES
            .iter()
            .find(|(l, _)| l == label)
            .map(|(_, c)| c)
            .unwrap_or_else(|| panic!("no case for {label}"));
        assert_matches(label, recorded, case);
    }
}
