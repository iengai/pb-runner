//! Pure parsers from Bybit v5 JSON (`serde_json::Value`, numbers as strings)
//! into the crate's types. Field choices mirror ccxt `bybit.ts` parse*
//! functions and passivbot's `exchanges/bybit.py` (PORT_INVENTORY section 3).
//!
//! Numbers are parsed with `str::parse::<f64>` (correctly rounded, the same
//! result as Python `float()`), never through serde_json's float parser (D8).

use crate::{
    Balance, ExchangeError, Fill, MarketSpec, OpenOrder, Position, PositionSide, Side, Ticker,
};
use crate::{Candle, MarginMode};
use serde_json::Value;

pub const QUOTE: &str = "USDT";
/// `market["limits"]["cost"]["min"] or 0.1` with ccxt 4.5.66 giving `None`.
pub const CCXT_MIN_COST_FALLBACK: f64 = 0.1;

/// `BTCUSDT` -> `BTC/USDT:USDT` (linear perpetual only; dated futures carry a
/// `-YYMMDD` suffix in ccxt and are excluded by [`parse_markets`]).
pub fn unified_symbol(base: &str, quote: &str) -> String {
    format!("{base}/{quote}:{quote}")
}

/// `BTC/USDT:USDT` -> `BTCUSDT`. Returns `None` for anything that is not a
/// USDT-settled linear perpetual symbol in unified form.
pub fn exchange_id(symbol: &str) -> Option<String> {
    let (base, rest) = symbol.split_once('/')?;
    let (quote, settle) = rest.split_once(':')?;
    if quote != QUOTE || settle != QUOTE || base.is_empty() {
        return None;
    }
    Some(format!("{base}{quote}"))
}

fn malformed(what: &str) -> ExchangeError {
    ExchangeError::Malformed(what.to_string())
}

/// Bybit encodes numbers as strings; empty string means "absent".
pub fn num(v: &Value, key: &str) -> Result<f64, ExchangeError> {
    opt_num(v, key)?.ok_or_else(|| malformed(&format!("missing numeric field `{key}`")))
}

pub fn opt_num(v: &Value, key: &str) -> Result<Option<f64>, ExchangeError> {
    match v.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) if s.is_empty() => Ok(None),
        Some(Value::String(s)) => s
            .parse::<f64>()
            .map(Some)
            .map_err(|_| malformed(&format!("field `{key}` is not a number: {s:?}"))),
        Some(Value::Number(n)) => Ok(n.as_f64()),
        Some(other) => Err(malformed(&format!("field `{key}` has type {other:?}"))),
    }
}

pub fn opt_u64(v: &Value, key: &str) -> Option<u64> {
    match v.get(key) {
        Some(Value::String(s)) => s.parse::<u64>().ok(),
        Some(Value::Number(n)) => n.as_u64(),
        _ => None,
    }
}

pub fn str_field<'a>(v: &'a Value, key: &str) -> Result<&'a str, ExchangeError> {
    v.get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| malformed(&format!("missing string field `{key}`")))
}

fn list(result: &Value) -> Result<&Vec<Value>, ExchangeError> {
    result
        .get("list")
        .and_then(Value::as_array)
        .ok_or_else(|| malformed("result.list missing"))
}

pub fn next_cursor(result: &Value) -> Option<String> {
    result
        .get("nextPageCursor")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

fn bool_field(v: &Value, key: &str) -> bool {
    match v.get(key) {
        Some(Value::Bool(b)) => *b,
        Some(Value::String(s)) => matches!(s.as_str(), "true" | "1" | "True"),
        Some(Value::Number(n)) => n.as_i64() == Some(1),
        _ => false,
    }
}

/// `/v5/market/instruments-info` (category=linear) -> USDT linear perpetuals
/// with status `Trading`. Fees are not in this payload; the caller fills the
/// account's rates (ccxt uses `fetchTradingFee` per symbol; passivbot reads
/// `markets[symbol]["maker"]` which ccxt seeds with 0.0002 / 0.00055).
pub fn parse_markets(
    result: &Value,
    maker_fee: f64,
    taker_fee: f64,
) -> Result<Vec<MarketSpec>, ExchangeError> {
    let mut out = Vec::new();
    for m in list(result)? {
        let settle = m.get("settleCoin").and_then(Value::as_str).unwrap_or("");
        let contract_type = m.get("contractType").and_then(Value::as_str).unwrap_or("");
        let status = m.get("status").and_then(Value::as_str).unwrap_or("");
        if settle != QUOTE || contract_type != "LinearPerpetual" || status != "Trading" {
            continue;
        }
        let base = str_field(m, "baseCoin")?;
        let quote = str_field(m, "quoteCoin")?;
        let lot = m
            .get("lotSizeFilter")
            .ok_or_else(|| malformed("lotSizeFilter missing"))?;
        let price = m
            .get("priceFilter")
            .ok_or_else(|| malformed("priceFilter missing"))?;
        let lev = m
            .get("leverageFilter")
            .ok_or_else(|| malformed("leverageFilter missing"))?;
        out.push(MarketSpec {
            symbol: unified_symbol(base, quote),
            id: str_field(m, "symbol")?.to_string(),
            qty_step: num(lot, "qtyStep")?,
            price_step: num(price, "tickSize")?,
            min_qty: num(lot, "minOrderQty")?,
            // ccxt 4.5.66 leaves limits.cost.min = None for Bybit linear; passivbot
            // then uses `or 0.1` (ccxt_bot.py set_market_specific_settings).
            min_cost: CCXT_MIN_COST_FALLBACK,
            min_notional: opt_num(lot, "minNotionalValue")?,
            contract_size: 1.0,
            max_leverage: num(lev, "maxLeverage")?,
            maker_fee,
            taker_fee,
        });
    }
    Ok(out)
}

/// `/v5/account/wallet-balance` -> passivbot's balance
/// (`exchanges/bybit.py::_get_balance`).
pub fn parse_balance(result: &Value) -> Result<Balance, ExchangeError> {
    let acct = list(result)?
        .first()
        .ok_or_else(|| malformed("wallet-balance list empty"))?;
    let account_type = str_field(acct, "accountType")?.to_string();
    let available = opt_num(acct, "totalAvailableBalance")?.unwrap_or(0.0);
    if account_type == "UNIFIED" {
        if let (Some(eq), Some(upl)) = (
            opt_num(acct, "totalEquity")?,
            opt_num(acct, "totalPerpUPL")?,
        ) {
            return Ok(Balance {
                total_usdt: eq - upl,
                available_usdt: available,
                account_type,
            });
        }
        let coins = acct
            .get("coin")
            .and_then(Value::as_array)
            .ok_or_else(|| malformed("coin list missing"))?;
        let mut total = 0.0;
        let mut used = false;
        for c in coins {
            if bool_field(c, "marginCollateral") && bool_field(c, "collateralSwitch") {
                used = true;
                total += opt_num(c, "usdValue")?.unwrap_or(0.0)
                    - opt_num(c, "unrealisedPnl")?.unwrap_or(0.0);
            }
        }
        if !used {
            return Err(malformed(
                "UNIFIED balance response has no enabled collateral coins",
            ));
        }
        return Ok(Balance {
            total_usdt: total,
            available_usdt: available,
            account_type,
        });
    }
    // Non-UTA: ccxt `total[USDT]` = coin.walletBalance for USDT.
    let coins = acct
        .get("coin")
        .and_then(Value::as_array)
        .ok_or_else(|| malformed("coin list missing"))?;
    let usdt = coins
        .iter()
        .find(|c| c.get("coin").and_then(Value::as_str) == Some(QUOTE))
        .ok_or_else(|| malformed("no USDT coin entry"))?;
    Ok(Balance {
        total_usdt: num(usdt, "walletBalance")?,
        available_usdt: opt_num(usdt, "availableToWithdraw")?.unwrap_or(available),
        account_type,
    })
}

fn pside_from_idx_or_side(v: &Value) -> Result<Option<PositionSide>, ExchangeError> {
    match opt_u64(v, "positionIdx") {
        Some(1) => return Ok(Some(PositionSide::Long)),
        Some(2) => return Ok(Some(PositionSide::Short)),
        Some(0) | None => {}
        Some(other) => return Err(malformed(&format!("invalid positionIdx {other}"))),
    }
    Ok(match v.get("side").and_then(Value::as_str) {
        Some("Buy") => Some(PositionSide::Long),
        Some("Sell") => Some(PositionSide::Short),
        _ => None,
    })
}

/// One page of `/v5/position/list`; flat (size 0) entries are dropped.
pub fn parse_positions(
    result: &Value,
    symbol_of: &dyn Fn(&str) -> Option<String>,
) -> Result<Vec<Position>, ExchangeError> {
    let mut out = Vec::new();
    for p in list(result)? {
        let size = opt_num(p, "size")?.unwrap_or(0.0);
        if size == 0.0 {
            continue;
        }
        let id = str_field(p, "symbol")?;
        let Some(symbol) = symbol_of(id) else {
            continue;
        };
        let pside = pside_from_idx_or_side(p)?.ok_or_else(|| malformed("position without side"))?;
        let entry = opt_num(p, "avgPrice")?
            .or(opt_num(p, "entryPrice")?)
            .unwrap_or(0.0);
        let margin_mode = match opt_u64(p, "tradeMode") {
            Some(0) => Some(MarginMode::Cross),
            Some(1) => Some(MarginMode::Isolated),
            _ => None,
        };
        out.push(Position {
            symbol,
            pside,
            size,
            entry_price: entry,
            leverage: opt_num(p, "leverage")?,
            margin_mode,
            updated_ms: opt_u64(p, "updatedTime"),
        });
    }
    Ok(out)
}

fn side(v: &Value) -> Result<Side, ExchangeError> {
    match v.get("side").and_then(Value::as_str) {
        Some("Buy") => Ok(Side::Buy),
        Some("Sell") => Ok(Side::Sell),
        other => Err(malformed(&format!("invalid side {other:?}"))),
    }
}

/// Position side of an order: `positionIdx` 1/2, else one-way derivation
/// (buy+!reduceOnly or sell+reduceOnly -> long).
fn order_pside(v: &Value, s: Side, reduce_only: bool) -> Result<PositionSide, ExchangeError> {
    if let Some(p) = pside_from_idx_or_side_order(v)? {
        return Ok(p);
    }
    Ok(match (s, reduce_only) {
        (Side::Buy, false) | (Side::Sell, true) => PositionSide::Long,
        _ => PositionSide::Short,
    })
}

fn pside_from_idx_or_side_order(v: &Value) -> Result<Option<PositionSide>, ExchangeError> {
    Ok(match opt_u64(v, "positionIdx") {
        Some(1) => Some(PositionSide::Long),
        Some(2) => Some(PositionSide::Short),
        Some(0) | None => None,
        Some(other) => return Err(malformed(&format!("invalid positionIdx {other}"))),
    })
}

/// One order object (from `/v5/order/realtime` list or a create response
/// merged with the request).
pub fn parse_order(
    v: &Value,
    symbol_of: &dyn Fn(&str) -> Option<String>,
) -> Result<Option<OpenOrder>, ExchangeError> {
    let id = str_field(v, "symbol")?;
    let Some(symbol) = symbol_of(id) else {
        return Ok(None);
    };
    let s = side(v)?;
    let reduce_only = bool_field(v, "reduceOnly");
    let client_id = v
        .get("orderLinkId")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    Ok(Some(OpenOrder {
        id: str_field(v, "orderId")?.to_string(),
        client_id,
        symbol,
        side: s,
        pside: order_pside(v, s, reduce_only)?,
        qty: num(v, "qty")?,
        price: num(v, "price")?,
        reduce_only,
        created_ms: opt_u64(v, "createdTime"),
    }))
}

pub fn parse_open_orders(
    result: &Value,
    symbol_of: &dyn Fn(&str) -> Option<String>,
) -> Result<Vec<OpenOrder>, ExchangeError> {
    let mut out = Vec::new();
    for o in list(result)? {
        if let Some(order) = parse_order(o, symbol_of)? {
            out.push(order);
        }
    }
    Ok(out)
}

/// `/v5/market/tickers` (category=linear), filtered to known markets.
pub fn parse_tickers(
    result: &Value,
    symbol_of: &dyn Fn(&str) -> Option<String>,
) -> Result<Vec<Ticker>, ExchangeError> {
    let mut out = Vec::new();
    for t in list(result)? {
        let id = str_field(t, "symbol")?;
        let Some(symbol) = symbol_of(id) else {
            continue;
        };
        let bid = opt_num(t, "bid1Price")?.unwrap_or(0.0);
        let ask = opt_num(t, "ask1Price")?.unwrap_or(0.0);
        let last = opt_num(t, "lastPrice")?
            .filter(|x| *x != 0.0)
            .unwrap_or(bid);
        out.push(Ticker {
            symbol,
            bid,
            ask,
            last,
            quote_volume_24h: opt_num(t, "turnover24h")?.unwrap_or(0.0),
        });
    }
    Ok(out)
}

/// `/v5/market/kline` rows `[start, open, high, low, close, volume, turnover]`
/// (strings, newest first) -> ascending candles.
pub fn parse_klines(result: &Value) -> Result<Vec<Candle>, ExchangeError> {
    let mut out = Vec::new();
    for row in list(result)? {
        let cols = row
            .as_array()
            .ok_or_else(|| malformed("kline row not an array"))?;
        if cols.len() < 6 {
            return Err(malformed("kline row has fewer than 6 columns"));
        }
        let mut c = [0.0; 6];
        for (i, slot) in c.iter_mut().enumerate() {
            let s = cols[i]
                .as_str()
                .ok_or_else(|| malformed("kline column not a string"))?;
            *slot = s
                .parse::<f64>()
                .map_err(|_| malformed(&format!("kline column {i} not numeric: {s:?}")))?;
        }
        out.push(c);
    }
    out.sort_by(|a, b| a[0].partial_cmp(&b[0]).unwrap_or(std::cmp::Ordering::Equal));
    Ok(out)
}

/// `/v5/execution/list` (execType=Trade) rows.
pub fn parse_fills(
    result: &Value,
    symbol_of: &dyn Fn(&str) -> Option<String>,
) -> Result<Vec<Fill>, ExchangeError> {
    let mut out = Vec::new();
    for f in list(result)? {
        let id = str_field(f, "symbol")?;
        let Some(symbol) = symbol_of(id) else {
            continue;
        };
        let s = side(f)?;
        let reduce_only = bool_field(f, "reduceOnly")
            || matches!(f.get("closedSize").and_then(Value::as_str), Some(x) if !x.is_empty() && x != "0");
        let pside = order_pside(f, s, reduce_only)?;
        out.push(Fill {
            id: str_field(f, "execId")?.to_string(),
            order_id: str_field(f, "orderId")?.to_string(),
            client_id: f
                .get("orderLinkId")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .map(str::to_string),
            symbol,
            side: s,
            pside,
            qty: num(f, "execQty")?,
            price: num(f, "execPrice")?,
            fee: opt_num(f, "execFee")?.unwrap_or(0.0),
            is_maker: bool_field(f, "isMaker"),
            timestamp_ms: opt_u64(f, "execTime").ok_or_else(|| malformed("execTime missing"))?,
        });
    }
    out.sort_by_key(|f| (f.timestamp_ms, f.id.clone()));
    Ok(out)
}

/// Number of decimals implied by a step such as `0.001` (ccxt
/// `precisionFromString`); used to format qty/price strings.
pub fn decimals_of_step(step: f64) -> usize {
    if step <= 0.0 {
        return 8;
    }
    let s = format!("{step}");
    if let Some(e) = s.find(['e', 'E']) {
        let exp: i32 = s[e + 1..].parse().unwrap_or(0);
        let mant_decimals = s[..e].split('.').nth(1).map_or(0, str::len) as i32;
        return (mant_decimals - exp).max(0) as usize;
    }
    s.split('.')
        .nth(1)
        .map_or(0, |d| d.trim_end_matches('0').len())
}

/// Format a value already rounded to `step` with exactly that many decimals
/// (Bybit rejects excess precision).
pub fn fmt_step(value: f64, step: f64) -> String {
    format!("{:.*}", decimals_of_step(step), value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn sym(id: &str) -> Option<String> {
        match id {
            "BTCUSDT" => Some("BTC/USDT:USDT".into()),
            "XRPUSDT" => Some("XRP/USDT:USDT".into()),
            _ => None,
        }
    }

    #[test]
    fn symbol_mapping() {
        assert_eq!(unified_symbol("BTC", "USDT"), "BTC/USDT:USDT");
        assert_eq!(exchange_id("BTC/USDT:USDT").as_deref(), Some("BTCUSDT"));
        assert_eq!(exchange_id("BTC/USDC:USDC"), None);
        assert_eq!(exchange_id("BTCUSDT"), None);
    }

    #[test]
    fn markets_filter_linear_perp_usdt() {
        let r = json!({"list": [
            {"symbol":"BTCUSDT","baseCoin":"BTC","quoteCoin":"USDT","settleCoin":"USDT","contractType":"LinearPerpetual","status":"Trading",
             "lotSizeFilter":{"qtyStep":"0.001","minOrderQty":"0.001","minNotionalValue":"5"},
             "priceFilter":{"tickSize":"0.10"},"leverageFilter":{"maxLeverage":"100.00"}},
            {"symbol":"BTCPERP","baseCoin":"BTC","quoteCoin":"USDC","settleCoin":"USDC","contractType":"LinearPerpetual","status":"Trading",
             "lotSizeFilter":{"qtyStep":"0.001","minOrderQty":"0.001"},"priceFilter":{"tickSize":"0.1"},"leverageFilter":{"maxLeverage":"100"}},
            {"symbol":"BTCUSDT-27DEC24","baseCoin":"BTC","quoteCoin":"USDT","settleCoin":"USDT","contractType":"LinearFutures","status":"Trading",
             "lotSizeFilter":{"qtyStep":"0.001","minOrderQty":"0.001"},"priceFilter":{"tickSize":"0.1"},"leverageFilter":{"maxLeverage":"100"}}
        ]});
        let m = parse_markets(&r, 0.0002, 0.00055).unwrap();
        assert_eq!(m.len(), 1);
        let b = &m[0];
        assert_eq!(b.symbol, "BTC/USDT:USDT");
        assert_eq!(
            (
                b.qty_step,
                b.price_step,
                b.min_qty,
                b.min_cost,
                b.max_leverage
            ),
            (0.001, 0.1, 0.001, 0.1, 100.0)
        );
        assert_eq!(b.min_notional, Some(5.0));
    }

    #[test]
    fn balance_unified_formula() {
        let r = json!({"list":[{"accountType":"UNIFIED","totalEquity":"18070.32797922","totalPerpUPL":"-0.11001349",
            "totalAvailableBalance":"17887.72614237","coin":[]}]});
        let b = parse_balance(&r).unwrap();
        assert_eq!(b.total_usdt, 18070.32797922 - -0.11001349);
        assert_eq!(b.available_usdt, 17887.72614237);
        assert_eq!(b.account_type, "UNIFIED");
    }

    #[test]
    fn balance_unified_coin_fallback() {
        let r = json!({"list":[{"accountType":"UNIFIED","totalEquity":"","totalPerpUPL":"",
            "coin":[{"coin":"USDT","usdValue":"100.5","unrealisedPnl":"0.5","marginCollateral":true,"collateralSwitch":true},
                    {"coin":"BTC","usdValue":"50","unrealisedPnl":"0","marginCollateral":true,"collateralSwitch":false}]}]});
        assert_eq!(parse_balance(&r).unwrap().total_usdt, 100.0);
    }

    #[test]
    fn positions_skip_flat_and_map_sides() {
        let r = json!({"nextPageCursor":"abc","list":[
            {"symbol":"BTCUSDT","side":"Buy","size":"0.5","avgPrice":"1073.85","positionIdx":1,"leverage":"10","tradeMode":0,"updatedTime":"1672282722429"},
            {"symbol":"XRPUSDT","side":"","size":"0","avgPrice":"0","positionIdx":2},
            {"symbol":"XRPUSDT","side":"Sell","size":"100","avgPrice":"0.5","positionIdx":2},
            {"symbol":"ETHUSDC","side":"Buy","size":"1","avgPrice":"3000","positionIdx":1}
        ]});
        let p = parse_positions(&r, &sym).unwrap();
        assert_eq!(p.len(), 2);
        assert_eq!(
            (
                p[0].symbol.as_str(),
                p[0].pside,
                p[0].size,
                p[0].entry_price
            ),
            ("BTC/USDT:USDT", PositionSide::Long, 0.5, 1073.85)
        );
        assert_eq!(p[0].margin_mode, Some(MarginMode::Cross));
        assert_eq!(p[0].updated_ms, Some(1672282722429));
        assert_eq!((p[1].pside, p[1].size), (PositionSide::Short, 100.0));
        assert_eq!(next_cursor(&r).as_deref(), Some("abc"));
    }

    #[test]
    fn open_orders_position_idx_and_one_way() {
        let r = json!({"list":[
            {"symbol":"BTCUSDT","orderId":"o1","orderLinkId":"x-pb-1","side":"Buy","qty":"0.010","price":"24674.7","positionIdx":1,"reduceOnly":false,"createdTime":"1692769133261"},
            {"symbol":"BTCUSDT","orderId":"o2","orderLinkId":"","side":"Sell","qty":"0.010","price":"25000","positionIdx":0,"reduceOnly":true},
            {"symbol":"BTCUSDT","orderId":"o3","orderLinkId":"","side":"Sell","qty":"0.010","price":"25000","positionIdx":2,"reduceOnly":false}
        ]});
        let o = parse_open_orders(&r, &sym).unwrap();
        assert_eq!(o.len(), 3);
        assert_eq!(
            (o[0].pside, o[0].client_id.as_deref(), o[0].created_ms),
            (PositionSide::Long, Some("x-pb-1"), Some(1692769133261))
        );
        assert_eq!(
            (o[1].pside, o[1].client_id.is_none(), o[1].reduce_only),
            (PositionSide::Long, true, true)
        );
        assert_eq!(o[2].pside, PositionSide::Short);
    }

    #[test]
    fn tickers_and_klines() {
        let r = json!({"list":[{"symbol":"BTCUSDT","bid1Price":"20517.96","ask1Price":"20527.77","lastPrice":"20533.13","turnover24h":"243765620.65899866"},
                               {"symbol":"BTCUSD","bid1Price":"1","ask1Price":"2","lastPrice":"3","turnover24h":"4"}]});
        let t = parse_tickers(&r, &sym).unwrap();
        assert_eq!(t.len(), 1);
        assert_eq!(
            (t[0].bid, t[0].ask, t[0].last),
            (20517.96, 20527.77, 20533.13)
        );
        let k = json!({"list":[["1670608800000","17071","17073","17027","17055.5","268611","15.74462667"],
                               ["1670605200000","17071.5","17073","17061","17071","4177","0.24469757"]]});
        let c = parse_klines(&k).unwrap();
        assert_eq!(c[0][0], 1670605200000.0);
        assert_eq!(
            c[1],
            [
                1670608800000.0,
                17071.0,
                17073.0,
                17027.0,
                17055.5,
                268611.0
            ]
        );
    }

    #[test]
    fn fills_sorted_ascending() {
        let r = json!({"list":[
            {"symbol":"BTCUSDT","execId":"e2","orderId":"o1","orderLinkId":"c1","side":"Buy","execQty":"0.1","execPrice":"1190.15","execFee":"0.071409","isMaker":true,"execTime":"1672282722430","positionIdx":1},
            {"symbol":"BTCUSDT","execId":"e1","orderId":"o1","orderLinkId":"c1","side":"Buy","execQty":"0.1","execPrice":"1190.10","execFee":"0.07","isMaker":false,"execTime":"1672282722429","positionIdx":1}
        ]});
        let f = parse_fills(&r, &sym).unwrap();
        assert_eq!(f[0].id, "e1");
        assert_eq!(
            (f[1].price, f[1].is_maker, f[1].pside),
            (1190.15, true, PositionSide::Long)
        );
    }

    #[test]
    fn step_formatting() {
        assert_eq!(decimals_of_step(0.001), 3);
        assert_eq!(decimals_of_step(1.0), 0);
        assert_eq!(decimals_of_step(0.10), 1);
        assert_eq!(decimals_of_step(1e-5), 5);
        assert_eq!(fmt_step(0.05, 0.001), "0.050");
        assert_eq!(fmt_step(3814.26, 0.01), "3814.26");
        assert_eq!(fmt_step(233.0, 1.0), "233");
    }
}
