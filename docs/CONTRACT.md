# Contracts

pb-runner has two external contracts: one with the passivbot engine it embeds,
one with the pbtb-rust control plane that launches it. Both are versioned
here; changes require a DECISIONS entry.

## 1. Engine pin

| Engine line | passivbot tag | commit | local checkout | fork branch for rlib build |
|---|---|---|---|---|
| 8 | `v8.1.0` | `7af64f3e930f4cf0cbb5fb88f31e9ce9594b0eb9` | `E:\projects\passivbot` | `iengai/passivbot` `pb-runner/rlib-v8.1.0` (P1.1) |
| 7 | `v7.12.0` | `fc6b9e016e04a3723bb5fe8847f1500049fa982c` | `E:\projects\pb-v712` | `iengai/passivbot` `pb-runner/rlib-v7.12.0` (P1.1, after line 8) |

- The runner calls exactly the same entry point the Python bot calls:
  `passivbot_rust::orchestrator::compute_ideal_orders(&OrchestratorInput)`
  (Python wrapper: `compute_ideal_orders_json`, `passivbot-rust/src/python.rs`).
- The runner's version string embeds the engine tag: `0.x.y+pb8.1.0`.
- Any engine bump goes through the "Upgrade procedure" in PLAN.md.

## 2. Container contract with pbtb-rust

Reference: `E:\projects\pbtb-rust\deploy\passivbot-image\{Dockerfile.ecs,entrypoint.sh}`.

| Item | Python image today | pb-runner image |
|---|---|---|
| Env in | `BUCKET`, `USER_ID`, `BOT_ID` | same |
| Startup | entrypoint downloads `s3://$BUCKET/$USER_ID/$BOT_ID/$BOT_ID.json` -> `/app/configs/$BOT_ID.json` and `.../api-keys.json` -> `/app/api-keys.json` (uses aws cli in image) | same files, same paths. Download done by the Rust binary itself (aws-sdk-s3) so the image needs no aws cli or shell; env names unchanged |
| Exec | `python src/main.py configs/$BOT_ID.json` | `pb-runner configs/$BOT_ID.json` |
| `api-keys.json` shape | `{ "<live.user>": {"exchange":"bybit","key":"…","secret":"…"}, "referrals": {…} }` | same; runner reads the entry named by `live.user` |
| Exit codes | 10 missing env, 20/21 download failed, 22 empty file | keep identical |
| Logs | stdout/stderr -> CloudWatch `/ecs/scalable-cluster-dev/passivbot[-v8]` | stdout JSON lines (tracing), same log-group convention with a `-rs` suffix (P6) |
| Memory reservation | 400 MB per task | to be measured; target <= 64 MB |
| Task-def family | `scalable-cluster-dev-passivbot` (7), `…-passivbot-v8` (8) | new families per line, selection per D7 |
| Egress | NAT instance fixed EIP (Bybit keys are IP-whitelisted) | unchanged |
| Write-back | none known (pbtb-rust observes ECS task state via EventBridge; balances via Bybit API from its own Lambda) | none. **Verify in P6** by grepping pbtb-rust for anything it reads that the container writes. |

## 3. Config contract

- Input config is the unmodified passivbot live config JSON (v8.x for the
  line-8 binary, v7.x for line 7). The `pbtb` top-level block (telebot
  metadata) and `strategy_name` are ignored, as the Python bot ignores them.
- `live.user` selects the key in `api-keys.json`; `live.approved_coins`,
  `live.ignored_coins`, `coin_overrides`, forager settings and bot params are
  consumed exactly as in the Python bot (docs/PORT_INVENTORY.md lists the
  reading code).
- A config with `live.fake_scenario_path` set means "fake exchange" (P5
  mock), never a real connection.
