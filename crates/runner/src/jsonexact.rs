//! JSON parser with correctly-rounded floats (`str::parse::<f64>`).
//!
//! serde_json's default float parser is best-effort and lands one ulp off for
//! some literals (docs/DECISIONS.md D8). Anything that compares recorded
//! Python-emitted numbers against runner-computed values must parse the
//! recording with this instead. The engine itself keeps using serde_json, on
//! purpose: that is what the Python bot's engine sees too.

use anyhow::{bail, Result};
use serde_json::{Map, Value};

struct P<'a> {
    b: &'a [u8],
    i: usize,
}

impl P<'_> {
    fn ws(&mut self) {
        while self.i < self.b.len() && matches!(self.b[self.i], b' ' | b'\n' | b'\r' | b'\t') {
            self.i += 1;
        }
    }

    fn expect(&mut self, c: u8) -> Result<()> {
        self.ws();
        if self.b.get(self.i) != Some(&c) {
            bail!("expected {:?} at byte {}", c as char, self.i);
        }
        self.i += 1;
        Ok(())
    }

    fn value(&mut self) -> Result<Value> {
        self.ws();
        match self.b.get(self.i).copied() {
            Some(b'{') => {
                self.i += 1;
                let mut m = Map::new();
                loop {
                    self.ws();
                    if self.b.get(self.i) == Some(&b'}') {
                        self.i += 1;
                        break;
                    }
                    let k = self.string()?;
                    self.expect(b':')?;
                    let v = self.value()?;
                    m.insert(k, v);
                    self.ws();
                    match self.b.get(self.i) {
                        Some(b',') => self.i += 1,
                        Some(b'}') => {
                            self.i += 1;
                            break;
                        }
                        _ => bail!("expected ',' or '}}' at byte {}", self.i),
                    }
                }
                Ok(Value::Object(m))
            }
            Some(b'[') => {
                self.i += 1;
                let mut a = Vec::new();
                loop {
                    self.ws();
                    if self.b.get(self.i) == Some(&b']') {
                        self.i += 1;
                        break;
                    }
                    a.push(self.value()?);
                    self.ws();
                    match self.b.get(self.i) {
                        Some(b',') => self.i += 1,
                        Some(b']') => {
                            self.i += 1;
                            break;
                        }
                        _ => bail!("expected ',' or ']' at byte {}", self.i),
                    }
                }
                Ok(Value::Array(a))
            }
            Some(b'"') => Ok(Value::String(self.string()?)),
            Some(b't') => {
                self.i += 4;
                Ok(Value::Bool(true))
            }
            Some(b'f') => {
                self.i += 5;
                Ok(Value::Bool(false))
            }
            Some(b'n') => {
                self.i += 4;
                Ok(Value::Null)
            }
            Some(_) => {
                let start = self.i;
                while self.i < self.b.len()
                    && matches!(
                        self.b[self.i],
                        b'0'..=b'9' | b'-' | b'+' | b'.' | b'e' | b'E'
                    )
                {
                    self.i += 1;
                }
                let t = std::str::from_utf8(&self.b[start..self.i])?;
                if t.is_empty() {
                    bail!("unexpected byte {:?} at {}", self.b[start] as char, start);
                }
                if t.contains(['.', 'e', 'E']) {
                    Ok(Value::from(t.parse::<f64>()?))
                } else if let Ok(i) = t.parse::<i64>() {
                    Ok(Value::from(i))
                } else {
                    Ok(Value::from(t.parse::<u64>()?))
                }
            }
            None => bail!("unexpected end of JSON"),
        }
    }

    fn string(&mut self) -> Result<String> {
        self.expect(b'"')?;
        let mut out = Vec::new();
        while self.i < self.b.len() {
            let c = self.b[self.i];
            self.i += 1;
            match c {
                b'"' => return Ok(String::from_utf8(out)?),
                b'\\' => {
                    let e = self.b[self.i];
                    self.i += 1;
                    match e {
                        b'n' => out.push(b'\n'),
                        b't' => out.push(b'\t'),
                        b'r' => out.push(b'\r'),
                        b'u' => {
                            let hex = std::str::from_utf8(&self.b[self.i..self.i + 4])?;
                            let cp = u32::from_str_radix(hex, 16)?;
                            self.i += 4;
                            let mut buf = [0u8; 4];
                            out.extend_from_slice(
                                char::from_u32(cp)
                                    .unwrap_or('?')
                                    .encode_utf8(&mut buf)
                                    .as_bytes(),
                            );
                        }
                        other => out.push(other),
                    }
                }
                other => out.push(other),
            }
        }
        bail!("unterminated string")
    }
}

/// Parse `text` into a `serde_json::Value` with exact float rounding.
pub fn parse_exact(text: &str) -> Result<Value> {
    let mut p = P {
        b: text.as_bytes(),
        i: 0,
    };
    let v = p.value()?;
    p.ws();
    if p.i != p.b.len() {
        bail!("trailing data at byte {}", p.i);
    }
    Ok(v)
}

#[cfg(test)]
mod tests {
    use super::parse_exact;

    #[test]
    fn exact_floats_and_types() {
        let v = parse_exact(r#"{"a": 0.19461776601265218, "b": 3, "c": [1.0, -2e-5, "x\"y"], "d": null, "e": true}"#).unwrap();
        assert_eq!(v["a"].as_f64().unwrap(), 0.19461776601265218_f64);
        assert!(v["b"].is_i64());
        assert_eq!(v["c"][0], serde_json::json!(1.0));
        assert_eq!(v["c"][1].as_f64().unwrap(), -2e-5);
        assert_eq!(v["c"][2], "x\"y");
        assert!(v["d"].is_null() && v["e"] == true);
    }
}
