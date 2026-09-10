# Status

Newest entry first. Each entry: what changed, what was verified, next action.

## 2026-09-10 -- the anchor result replicates across three accounts

**One account was a story; three are a measurement.** The shadows were
restarted onto the anchor-logging build at 09-09 09:13 UTC, which gave all
three a fresh cold-start anchor at the same moment. Twenty-one hours and ~29k
cycles later, at 06:10 UTC:

    shadow              py snap    shadow anchor    gap       reconciliation
    dollardigger_v8ref   330.43       330.4639    +0.010%    ideal=5 matched=5
    xxbot               1003.33      1004.3246    +0.099%    ideal=5 matched=4 cancels=1
    abot                 263.90       264.4833    +0.221%    ideal=3 matched=2 cancels=1

`order_match_tolerance_pct` is 0.0002. The one shadow whose anchor sits inside
that tolerance agrees on every order; the two outside it disagree on exactly
one order each, and in both cases that order is `entry_grid_cropped_long` --
the single balance-dependent order in the wave. xxbot: DOGE 9062.0 vs 9035.0
(+0.30%), price one tick apart. abot: XRP 644.3 vs 642.1 (+0.34%), price
identical.

xxbot is the useful one. Before the restart it was clean at `ideal=1
matched=1`; the restart handed it a new anchor and the same signature appeared
on a different account, a different coin, and a different order count. That is
the mechanism reproducing on demand rather than one account's coincidence.

**What this does and does not license.** It closes the abot question: the
standing shadow disagreement is a cold-start anchor artifact, parity-correct on
both sides, and not a port defect. It does not make shadows useless -- five of
five and four of five orders still match exactly, which is the actual parity
result -- but it does put a floor under them: any balance-dependent order is
uncomparable while the anchors differ by more than the match tolerance, and
neither anchor moves until raw travels the configured 1% band. A shadow
started at the same moment as the bot it shadows would not have this problem;
one started 14 minutes later has it for as long as both processes live.

**Unrelated, and open: paper2 plans nothing for minutes at a time.** In the 24
hours to 06:00 UTC the live rs runner on `467146583` logged 518 warnings in
five bursts (09-09 06, 07, 08, 15 and 20 UTC), each a minute or two long, every
cycle in the burst pairing `StrategyInputUnavailable { pside: Long, scope:
StrategyOrders }` with `no ideal orders this cycle` -- while holding a position
(`long=1.7@1.4`, balance 42.16). No orders are planned at all during those
windows. Python has a visibly similar state (`[trailing] trailing state
unavailable reason=missing_exact_trailing_candles ... until_fresh`), so this
may well be parity-correct, but the two have not been compared at the same
moment and nothing here establishes that they enter and leave the state
together.

**That comparison arrived the same day, from the abot shadow.** At 14:04 UTC
abot's Python bot closed its XRP position and immediately reopened it
(`10.7 @ 1.3653`), then logged `[trailing] trailing state unavailable
reason=missing_exact_trailing_candles symbols=XRP
action=mark_trailing_branches_unavailable_until_fresh` at 14:04:27 and posted
nothing after it. The rs shadow on the same account, holding that same
`long=10.7@1.3653`, was logging `StrategyInputUnavailable { pside: Long, scope:
StrategyOrders }` with `ideal=0` across the whole window. Same account, same
minute, same position, both runtimes refusing to plan, and Python naming the
reason: the trailing branches need candles anchored at the new position and
they are not there yet.

That is entry into the state matched directly rather than inferred. It is not
yet exit -- as of 14:05:54 neither had recovered, so what the comparison shows
is that they stop together, not that they resume together. The remaining
question is whether one waits materially longer than the other, which needs a
window that has closed on both sides.

The other live rs runner
(`452425891`, 3 coins) logged one warning in the same period, a rate-limited
candle refresh that fell back to cached candles.

**Also this period:** 0 ERROR, 0 write failures and 0 `110094` across both live
rs runners in 24h. Memory, two-hour average: rs 17.0 MiB live and 18-20 MiB
per shadow, against Python's 375.5 (v8) and 403.5 (v7).

## 2026-09-09 -- PnL cannot measure the runtime; a shadow on abot can

**The comparison that looked controlled is not.** abot and paper2 run
byte-identical configs -- 232 keys, the only difference being `live.user` --
and paper2 moved to the Rust runtime while abot stayed on Python. That reads
like a runtime A/B and is not one. paper2 holds ~42 USDT against abot's ~264,
so paper2's orders sit against the exchange minimum while abot's have room,
and over the thirteen days when BOTH were still on Python paper2 returned
0.13%/day against abot's 0.62%/day. Five to one, before any runtime changed.
Whatever separates those two accounts was there first; no amount of waiting
separates it from the runtime afterwards.

**What can be measured instead.** A shadow on abot: the same account, the same
moment, the same orders on the book, one runtime planning against the other's
book. The metric is the reconciliation itself -- `cancels=0 creates=0
matched=N` means the two runtimes wanted the same orders -- not a PnL
difference that needs weeks to clear its own noise.
`tools/bybit_account_report.py` (read-only, runs on the NAT) is still useful
for balances and realized PnL; it just cannot answer this question.

**Shadow up, and it disagrees.** `passivbot-v8-rs-shadow-abot`, dry-run,
`build=8133c5c…`, started 08:26:46 UTC. Every cycle since: `ideal=3 cancels=1
creates=0 matched=2 deferred=1`, wanting to cancel `Buy Long qty=620.2
price=1.3592 entry_grid_cropped_long` -- which the Python bot posted at
08:12:36 UTC, 14 minutes before the shadow started, and has held without
replanning ever since. The two *normal* grid entries in the same wave
(`47.2@1.4146`, `267.7@1.3908`) match exactly. Only the cropped one differs,
by more than `order_match_tolerance_pct` (0.02%). The other two shadows are
clean at the same moment: xxbot `ideal=1 … matched=1` at cycle 22633,
dollardigger_v8ref `ideal=6 … matched=6` at cycle 22807 -- so this is abot's
cropped entry, not a general shadow artifact.

**Cause not determined.** Candidates, in no order: a port defect in
cropped-entry sizing (the crop is the term most sensitive to balance, so a
small input difference shows up there and nowhere else); an input difference
the shadow inherits from starting cold; or drift that is real and that
Python's own replace thresholds tolerate while ours does not. The repeating
`deferred=1` is NOT a second symptom -- it is the 2.7 cancel-first barrier
holding the replacement create behind a cancel that a dry run never performs,
so the state cannot advance and the same disagreement reprints forever.

**A hypothesis with a number attached, written before the measurement.**
Balance goes through `passivbot_rust::utils::hysteresis`: the snapped balance
only moves when raw leaves a band around it (`balance_hysteresis_snap_pct`,
2% by default), and `prev_hysteresis_balance == 0.0` on cycle 1 means a
cold-started bot anchors on whatever raw was at startup. abot's Python bot
reports `bal=264.47 USDT (snap 263.90)` -- its anchor is 0.216% below raw and
has been for some time. The shadow started at 08:26:46 and anchored on raw.
Two anchors 0.216% apart, and neither moves until raw travels the band, which
is why the disagreement is static rather than drifting. (These bots configure
`balance_hysteresis_snap_pct = 0.01`; 2% is upstream's default, not their
setting -- corrected 2026-09-10, the shadow's own anchor line reports it.)

That predicts the signature exactly. Grid re-entry quantities come from the
position, not the balance, so the two normal entries are identical -- as
observed. The cropped entry is the residual against the wallet-exposure limit,
so it absorbs the whole balance difference: 0.216% of ~1296.6 USDT of planned
notional is ~2.80 USDT, or ~2.06 XRP at 1.3592. **The shadow's deferred create
should be ~622.2-622.4 @ 1.3592 against Python's 620.2.** If it is, this is a
cold-start artifact of shadowing and not a port defect. If the qty is 620.2
and the PRICE differs, or the gap is not ~2, the hypothesis is dead and the
crop arithmetic is the next place to look. Stated first, measured second --
D25's evidence was the other way round.

**Next action, and it is an observability fix first.** The dry-run path logs
cancels and creates but not *deferred* creates, so the shadow can say it
disagrees and cannot say what it wanted instead -- the disagreement is
unreadable from the log alone. Log deferred creates with their bucket
(`barrier`/`recent`/`churn`/`capacity`) -- done, `9566e89`, calibrated by
removing the recording and watching the test fail. The three shadows were
redeployed onto that build and the measurement is above.

**Measured: `qty=622.5 price=1.3589 reason=barrier`.** The prediction was
622.2-622.4 @ 1.3592. The quantity landed one 0.1 step -- 0.04% -- above the
band; the price moved by three ticks, 0.022%, just past the 0.02% match
tolerance that makes this a replace at all.

**The falsification clause was badly formed, and that matters more than the
hit.** "If the price differs, the hypothesis is dead" was written without
checking whether the grid price depends on balance. It does:
`calc_entry_distance_multiplier` feeds `wallet_exposure /
effective_wallet_exposure_limit` into the distance multiplier
(`entries.rs:159-180`), and wallet exposure is position notional over balance.
A balance difference moves the price as well as the quantity, so the clause
would have discarded the hypothesis for confirming itself. Only the quantity
half of the prediction carried information. It was right to within one step.

**Parity-correct, not a port defect.** Python seeds the same anchor the same
way -- `previous_hysteresis_balance is None -> balance_raw` (passivbot.py:16156)
-- so both runtimes cold-start on whatever raw was at process start and hold it
until raw travels the configured band -- 1% on these bots. Two processes on
one account can therefore size against
balances that differ by up to the band, indefinitely. For a shadow that is
structural: it will read as a standing disagreement on every balance-dependent
order for as long as the anchors differ, and it is not evidence about the port.

**Still an inference, so the anchor is now logged.** Everything above reads the
cause backwards out of the outputs. `[balance] hysteresis anchor moved`
(`balance`, `balance_raw`, `previous_anchor`, `snap_pct`) makes the one
variable directly observable at both ends, against Python's own
`bal=264.47 USDT (snap 263.90)`. It fires on change, which for a sticky anchor
means once at startup and rarely after.

**Closed, by direct observation of the one variable.** Both ends, same minute:

    py     09:05:49  bal=264.48 USDT (snap 263.90)
    shadow 09:13:17  balance=264.48327615 balance_raw=264.48327615
                     previous_anchor=0.0 snap_pct=0.01

Identical raw balance; anchors 0.221% apart, because the shadow cold-started
onto raw and Python has been holding an older anchor. Not inferred from the
outputs this time -- read off both processes.

The arithmetic closes too. By then Python had replaced its own order (09:00:16,
`Δp=0.0221% Δq=0.0322%`) and the two runtimes' prices agreed exactly at 1.3589,
leaving the entire residual in the balance-dependent quantity: 622.5 against
620.4, a gap of 2.1 XRP against the 2.06 predicted from a 0.22% balance
difference -- one qty step, which is the instrument's resolution.

**Correction to the entry above:** the band is `snap_pct=0.01` in these configs,
1%, not the 2% library default quoted before. It does not change the conclusion
-- 0.22% is inside either -- but 2% was the wrong number to write down.

**xxbot, meanwhile, disagreed and then stopped.** It showed `ideal=1 cancels=1
matched=0` for a few minutes after its Python bot moved an order, and converged
back to `matched=1 deferred=0` on its own. That is the shape a timing
difference makes: transient. abot's is the other shape -- static, because
neither anchor will move until raw travels 1%.

**So: a property of shadowing, not a finding about the port.** Both runtimes
seed the anchor identically on cold start, so the same gap opens whenever a
Python bot is replaced by a Rust one mid-life; it is bounded by the band, it
affects only balance-dependent orders, and it closes the next time raw leaves
the band. Seeding a shadow from the live bot's anchor would make the comparison
read cleaner and the shadow less faithful to the thing it stands in for, so it
is not obviously worth doing. What was worth doing is being able to see it:
before this, the shadow could report a disagreement and name neither the order
nor the cause.

## 2026-09-08 -- what the broker code actually is, and making it a choice (D26)

**It pays, per order.** Bybit's API Broker FAQ describes attribution as
per-ORDER: the broker sends its id on every order request, Bybit totals the
volume by id and pays rebates on it (up to 45% of the fee by tier). passivbot
states the partner relationship openly in its README and its Bybit connector
refuses to start without a code. So the header D25 added is both what waives
the minimum notional and what decides who earns from this volume -- every
order these bots place is attributed to passivbot, as the Python bots always
have been.

**Changed:** `BybitConfig.broker_id` defaults to `passivbotbybit` and is
overridable by `PB_RUNNER_BROKER_ID`; empty sends no header, as ccxt does with
an unset `brokerId`. Startup logs `[config] broker attribution broker_id=…`.
An env var rather than a config key, because those config objects in S3 are
the same ones the Python bots read. A test pins that an empty code sends no
`Referer`, since getting that wrong fails silently -- Bybit just starts
enforcing the minimum again.

**Open question, one run away:** whether the waiver follows passivbot's code
specifically or any valid broker code. That decides whether attribution can be
moved without losing the ability to trade small accounts.
`tools/bybit_request_probe.py --broker-id <other>` answers it.

**Not done: the probe has never been run.** It places a real order (0.1 XRP at
10% under the mark, ~0.13 USDT, cancelled immediately) and placing orders is
not something this agent does; the dry run on the NAT is green, so it is one
command away for a human. Its value is no longer the D25 question -- that is
settled by a filled order -- but the controlled A/B that D25's before-and-after
evidence never was.

## 2026-09-08 -- the reduceOnly answer was wrong; the broker header is next (D25)

**The confirming run ran and disproved it.** paper2 flat, rs on
`build=cef25b8c…` (the merged build with all three D24 parity fixes), 15:21:27
UTC: `qty=1.6 price=1.4148` -- 2.26 USDT -- rejected `110094` again, and again
15 s later. Dropping `reduceOnly` changed nothing.

**What went wrong in the reasoning:** "the only difference I could find" was
treated as "the difference", when it was really a statement about where I had
looked -- the harness compared method, path, query and body, and had not been
extended to headers. The three fixes are all real divergences and all still
correct; none was the fault. The cost was acting on a hypothesis (merge,
build, swap the live bot) at the confidence of a proof.

**What was left, and is now sent:** ccxt attaches the broker id as a `Referer`
header on POST and only POST (`bybit.py:9424-9427`), from
`options["brokerId"]`, which `exchanges/bybit.py::create_ccxt_sessions` sets
to `passivbotbybit` and refuses to start without. We sent none. POST-only
scope matches the failure surface exactly: our GETs have never been rejected,
and neither have our >5 USDT orders.

**Also changed:** the fixture records headers (sign/clock/key collapsed to
`<volatile>`), and the parity test asserts every header ccxt sets
deliberately. The assertion was calibrated first -- removed the header, watched
it fail, put it back.

**CONFIRMED at 15:34:55 UTC.** Built `b8af7e9a…`, promoted `v810` to it,
stopped the old task and ran a new one on the same account, still flat:

```
planned cycle=1 ideal=1 cancels=0 creates=1
[order] post XRP/USDT:USDT Buy Long qty=1.6 price=1.4152 entry_initial_normal_long
wave done cancels_ok=0 creates_ok=1 failures=0
```

Accepted and filled (`long=1.6@1.4152` in the cycles after). Zero `110094`
and zero unacknowledged creates in the task. Thirteen minutes earlier the same
account rejected the same size; the only difference is the header.

**So:** Bybit enforces the per-symbol `minNotionalValue` on orders without
passivbot's broker `Referer` and waives it on orders with it. The broker code
is not a fee-rebate cosmetic -- it changes the account's trading limits. This
never showed on the larger `8rs` account because every order it plans is
already above 5 USDT.

**Next action:** the other live rs bot (`452425891`) is still on the old build
and still sends no `Referer`. It is unaffected today (all its orders clear 5
USDT), but it should be restarted onto `v810` at a convenient moment. paper2
is running the new build and healthy (`ideal=3 matched=3`).

## 2026-09-08 -- a Bybit order field we send and the Python bot never has (D24)

**What broke:** paper2 (`467146583`) closed its XRP long at 14:18:01 UTC and
could not open another. Every cycle planned the same `entry_initial_normal_long`
(`qty=1.6 price~1.408`, 2.25 USDT) and Bybit rejected every one with
`110094 Order does not meet minimum order value 5USDT`. Ten per hour exhausts
the error budget, so the bot restarted every ~3 minutes and was ~25 minutes
from `max_n_restarts_per_day` and a code-30 exit the restart lambda would not
have revived.

**The first answer was wrong.** It reasoned -- correctly, at every step -- that
ccxt leaves `limits.cost.min` unset for Bybit linear, so `min_cost` is 0.1, so
`effective_min_cost` is 0.14 USDT, so `filter_by_min_effective_cost` cannot
fire, so the Python bot would fail identically and the account was simply too
small. It was overturned by one experiment: the Python 8.1.0 bot on the SAME
account filled `1.6 @ 1.4084` -- the same 2.25 USDT -- twelve minutes later.

**The actual difference,** from building ccxt's request offline and diffing:
we send `reduceOnly`, the Python bot does not, and never has.
`_build_order_params` returns exactly `{positionIdx, timeInForce, orderLinkId}`;
closes are expressed by `positionIdx` alone, since in hedge mode a Sell on
positionIdx 1 can only reduce the long. D11's inventory claimed `reduceOnly`
was reproduced verbatim from the Python adapter; that line is now corrected.

**Changed:** `order_body` drops the field. Reconciliation does not move --
`normalize_open_order` already derives reduce-only from `(pside, side)` in
hedge mode and ignores what the exchange reports, exactly as Python does.
Workspace tests pass.

**Not yet proven, and it matters:** that this field is what Bybit's validator
branches on. The two attempts were twelve minutes apart, so an account-side
change is not excluded by them alone. **Next action:** with paper2 flat, run
the rs bot on the new build and watch whether a sub-5-USDT `entry_initial` is
accepted. Until then that account cannot place an entry from the runner.

**The gap this exposed is now closed.** All four existing harnesses replay
recorded inputs and compare plans; a request body is not a plan, and nothing
had ever compared the bytes we POST against the bytes ccxt would POST -- the
parity check D11 wrote down for itself and never ran. There is now a fifth
harness: `tools/ccxt_request_fixtures.py` records what ccxt would send for all
seventeen call sites passivbot uses (dummy key, canned private responses,
nothing placed, nothing read), and
`crates/exchange-bybit/tests/ccxt_request_parity.rs` drives our client against
a local socket and compares method, path, query and body key by key, as TEXT,
so `10` and `"10"` are different requests. Deliberate differences live in
`CASES` with a reason.

**It found two more divergences on its first run,** both fixed here:
`fmt_step` padded prices to the tick's decimals (`"1.5000"` where ccxt's
NO_PADDING gives `"1.5"`), and `set_margin_mode` carried `leverage` as a
string where ccxt passes passivbot's `int` straight through. Three divergences
in seventeen requests, in a client whose inventory claimed the surface was
reproduced verbatim.

## 2026-09-08 — deploys stop going through terraform (D23)

**Why:** rolling out a runner fix meant editing `image_tag` in the pbtb-rust
tfvars, a scoped apply, `telebot-deploy`, then a restart -- a cadence
inherited from the Python image, where one pin per upstream release records
real history. pb-runner ships fixes far more often (D21 and D22 were both
same-day), and the pin made every one of them an infrastructure change.

**Changed:** the image now carries a moving tag named after the passivbot
version line it serves, `v810`, and `passivbot_engines["8rs"]` points at that
instead of a commit. `image-build` gained a `promote` job that re-points the
tag after each build (manifest copy, no rebuild; runs even when the build was
skipped as already-built) and a `promote: false` input for a build that
should not become deployable. `docker/Dockerfile` bakes the commit into
`PB_RUNNER_BUILD` and the binary logs
`pb-runner starting version=… engine_line=… build=<git sha>` as its first
line -- once the tag moves, that is the only record of which build a
container ran.

**So a fix is now:** run `image-build`, then Stop/Run the bot in telebot. ECS
re-pulls the tag on every task start. Terraform is for adding a version line
(a v8.2.0 or v7.1.2 runner: its own tag, its own engine entry).

**Known cost:** an auto-restart after a crash resolves the tag afresh, so a
bot that dies right after a build comes back on the new build. Rollback is
re-pointing `v810` at one of the immutable `<git sha>` tags (pbtb-rust
RUNBOOK, "Shipping a pb-runner fix"), bounded by `keep_last_images = 20`.

**Rolled out:** the apply landed `passivbot-v8-rs:4` (image
`pb-runner:v810`, memory 64), the lambda's engine table reads
`8rs=…-v8-rs:4`, and telebot-deploy 34221161471 wrote the same revision into
`/etc/telebot/telebot.env`. pbtb-rust PR #40 is merged (`9197e13`), so `main`
and the dev state agree again; a targeted plan on the line reports no
changes. `v810` points at commit `4750ce5` -- D22 and the startup log
included -- and shares its digest with the `src-…` and `<git sha>` tags.

**Two things went wrong on the way, both now fixed in the workflow:**
`aws ecr put-image` was handed the manifest through a shell variable and
rejected it ("Invalid JSON syntax") because ECR returns it pretty-printed;
the retry then succeeded but put `v810` on a SECOND image index, since the
manifest ECR returns is not byte-identical to what was pushed. Retagging is
now `docker buildx imagetools create`, and the step asserts the two raw
manifests match before reporting success (D23 item 5). The stray index is
untagged and expires under the 7-day rule.

**A drift check people were using is now void** (D23 item 6): tfvars
`image_tag` versus the running digest is an identity today. The build a
container is running is answerable only from its first log line,
`build=<git sha>`.

**Needs the user:** restart both `rs` bots from telebot (Stop, then Run) --
they are still on `:3`.

## 2026-09-08 — D21 verified live; exchange clock alignment (D22)

**D21 verified on the live bots.** Both `8rs` bots restarted on task
definition `passivbot-v8-rs:3` (image `pb-runner:ca832b6`, built on the new
GitHub arm64 runner) and healed immediately:

- `452425891`, 3 symbols: `[trailing] backfilled the candles the position
  anchor needs symbol=XRP anchor_ms=1788513720000 m1=5801` (the buffer grew
  from 2341 to 5801 minutes), then `cycle=1 ideal=4 creates=3` -- including
  the `close_grid_long` the position had been sitting without -- and
  `ideal=4 warnings=0` on every cycle since.
- `467146583`: same backfill line (anchor 3263 minutes back), `ideal=3`,
  three entries posted, one of which filled at 10:05:11.

**One expected-looking gap, checked and not a bug:** right after that fill,
`467146583` logged `StrategyInputUnavailable` for 109 s and cancelled its two
entries before reposting them. That is D17's rule, and Python's: the bundle
needs one COMPLETE minute after the anchor (`missing_exact_trailing_candles`,
pb:9651), so a side is unavailable from a fill until the following minute
closes. Not a regression -- but the log did not say so, and it reads exactly
like the D21 symptom.

**Changed (D22):** the Bybit client now carries a `time_offset_ms`
(`server - local`, from the public `/v5/market/time`, measured at the round
trip's midpoint) and adds it to every signed timestamp; a `10002` rejection
triggers one re-sync and one retry, and nothing else does. `LiveRunner`
syncs at startup and on the hourly maintenance cycle, warns above 500 ms, and
treats a failed sync as non-fatal. Bybit rejects a timestamp more than 1 s
AHEAD of its clock -- `recv_window` bounds only lateness -- so a fast host
clock fails every private call; that is what killed the D21 soak
(`exit=30` after 11 restarts) and what ended the local dry-run against
paper2 (+1021 ms).

**Verified:** `cargo test --workspace --all-features` 141 (new:
`timestamp_errors_are_the_only_resync_trigger`,
`signed_timestamps_carry_the_synced_offset`,
`the_hourly_cycle_resyncs_the_exchange_clock`); clippy clean; mockrun
`old_anchor` 120/120 and `public` 600/600 requests, account state and engine
inputs identical.

**Next:** build the image with D22 and roll it onto `8rs`; merge
`iengai/pbtb-rust` PR #40 (the OIDC build role, the ECR lifecycle policy and
the `8rs` image tag are applied in dev but not on `main`); restart the soak.

## 2026-09-08 — live incident: the `8rs` bots planned nothing for a position older than their warmup window (D21)

**What happened:** the two bots moved to the Rust runtime (paper2
`467146583`, `452425891`) cancelled all four resting orders on their XRP
position in cycle 1 and then planned nothing at all -- `ideal=0 cancels=0
creates=0 warnings=1` for 330 / 51 cycles. Bybit confirmed the state on
paper2: long 2.7 XRP (3.74 USDT, entry 1.4309, mark 1.3854, -32.7% ROI),
**zero open orders**, balance 42.02 USDT. An unmanaged position with no
close order.

**Cause (D21):** the trailing bundle was folded out of the warmup 1m buffer,
but the anchor (newest fill of the side) was older than that buffer -- 3070
min against a 2872-minute warmup, and 5621 against 2340 -- so
`trailing_bundle` returned `None`, `trailing_available = false`, and the
engine emitted no orders for the side (`StrategyInputUnavailable`). Nothing
refetches that history, so the state never heals. Python fetches the
trailing candles from the anchor itself (SNAPSHOT_SPEC 4.1 step 3).

**Diagnosis path, and the two dead ends worth remembering:** the first local
"reproduction" came from a stale binary (built before the day's commits) and
was void; `live.balance_override` is not implemented, so the balance
experiment built on it was void too. What settled it was the deployed image
itself, pulled from ECR and run under QEMU against the read-only abot key
with paper2's config: `ideal=3 warnings=0`, healthy -- image and config
exonerated, leaving account state. paper2's own key is IP-bound to the NAT
(`10010 Unmatched IP`), so the account state came from the Bybit UI plus the
arithmetic above.

**Changed:** `live.rs` `ensure_trailing_candles` backfills 1m candles from
the anchor each cycle before the snapshot is built, widening the symbol's
buffer (`trailing_floor_ms` / `m1_keep`, capped by
`live.max_memory_candles_per_symbol`); `live.recv_window_ms` is finally
passed to the Bybit client (every signed request had used the 5 s default);
new logging -- engine warnings by name, an empty plan dumps balance /
positions / `tradable` / `effective_min_cost` / `symbol_states` /
`loss_gate_blocks`, warmup logs the candle counts it got, the
missing-strategy-input fallback names the symbol.
`tools/record_fake_v8.py --seed-fill-age-minutes` (default 1, unchanged)
ages the seeded entry fill, which is the trailing anchor.

**Harness gap:** every recording started flat and each seeded fill was
stamped one minute before boot, so the anchor always fell inside the warmup
window -- 3400 compared cycles never touched this branch. Fixture
`grid_v7_old_anchor` (6000-minute fill age) records it.

**Verified:** `cargo test --workspace` 138 (new:
`trailing_candles_are_backfilled_to_an_anchor_older_than_the_buffer`).

**Next:** finish the `grid_v7_old_anchor` recording + mockrun/diffcheck
against it, rebuild the arm64 image, and roll the two bots onto it. Not yet
implemented: Python's coarse-resolution prefix for very old anchors, and
`live.balance_override`.

## 2026-09-08 (worktree agent) — HSL coin mode ported (`hsl_coin.rs`, D20); `realized_pnl_cumsum` fee fallback (D16.5)

**Changed:** `crates/runner/src/hsl_coin.rs` (new): the per-pair HSL
machine of `live.hsl_signal_mode = "coin"` -- `CoinState` / `CoinMetrics`
/ `CoinStopEvent`, `HslState::{check_coin, supervise_coin_red,
initialize_coin_from_history, coin_panic_pairs, coin_red_active,
finalize_coin_red_stop, reset_coin_after_restart,
handle_coin_position_during_cooldown, refresh_coin_cooldown_after_repanic}`,
`coin_realized_pnl_peak_last`, `coin_replay_events`,
`coin_bounded_required_replay_start_ts`, `infer_coin_replay_contract`,
`compact_sparse_replay_indices`, `RealizedWindow`; `CoinEnv` (unrealized
pnl, blocking-order counts, optional traced realized override). `hsl.rs`:
config accepts coin mode (D16 item 2 lifted), `coin_overrides` /
`side_config` / `coin_active_pside`, `HslFill.pb_order_type` + `is_panic`,
symbol-aware `latest_flatten_fill_timestamp`, `LatchPayload.complete`,
`HslModes.{coin_enabled, replay_pending, runtime_forced}`,
`HslState.{coin, runtime_forced, replay_pending}`, and the fill replay
generalised into `coin_history` (`ReplayInputs`, `CoinHistory`,
`PanicFlatten`, `psize_after_quirk`) with `balance_equity_timeline` as its
timeline wrapper. `snapshot.rs`: `mode_override` steps 2-3 from
`HslModes`, `SnapshotBuilder::build_protective` (the coin RED supervisor's
reduced input, pb:16516). `live.rs`: coin initialization at warmup over
`coin_history`, per cycle `check_coin` + one supervisor iteration +
protective planning / reconciliation when pairs stay under panic
supervision, `blocking_orders_symbol`, `coin_upnl`, `hsl_fills` decodes
`pb_order_type` from the fill's client id. `bin/snapcheck.rs`: coin trace
replay (`compare_coin_state(s)`, `compare_coin_modes`, traced per-pair
inputs with the ledger cross-check, flatten-lookup cross-check,
`run_protective`). Tools: `fake_live_clock.py` `coin_*` trace records and
the `protective` flag on `compute`; `select_fixtures.py` coin transition
keys and the coin trace compaction. Fixtures
`tests/fixtures/configs/fake_v8/grid_v7_hsl_coin.json`,
`tests/fixtures/recordings/fake_v8/grid_v7_hsl_coin` (25 cycles + trace +
fills + scenario, 3.2 MB). Docs: SNAPSHOT_SPEC 2.3 steps 2-3 / 8, PLAN
P4.1 / P4.2, RECORDER A, fixtures README, D20. Second commit:
`live::realized_pnl_cumsum` takes `FeePolicy` + `c_mult` and applies the
fill manager's zero-fee fallback / sanity replacement like `hsl_fills`
(SPEC 5.2, D16 item 5), `LiveRunner.fee`; unit test.

**Verified:** full coin run (`.local/fake_v8_hsl_coin/grid_v7_hsl_coin`,
boot 2025-10-10 20:00, 400 cycles, 402 computes with 2 duplicate
protective inputs): `pb-snapcheck` 400/400 identical including the two
protective-panic recordings, "every traced state matches" over 1 init /
400 checks / 2 supervisor runs (8 iterations) / 2 finalizations / 2 resets /
4 flatten lookups, 87479/87479 floats bit-exact, 8 realized-pnl inputs
explained by the stale ledger (the harness refreshes fills only inside the
supervisor's flatten lookup). Per-coin timeline: ADA/DOGE yellow 58, orange
61/62 (`tp_only`), red 68/70 (`panic`, finalized inside the cycle,
`graceful_stop`), reset 188/190; BTC (red 0.3) green throughout. Committed
subset 25/25. Regression: `pb-snapcheck` grid_v7, tm, grid_v7_seeded,
tm_seeded, grid_v7_forced 30/30, grid_v7_hsl 27/27 (every traced state
matches, 10822/10835 bit-exact); `pb-plancheck` public grid_v7 600/600, tm
600/600, seeded2 grid_v7 / tm / tm8 / iter7 400/400; `pb-mockrun
--diff-inputs` (after the fee fallback) public grid_v7 600/600, tm 600/600,
seeded2 grid_v7 / tm / tm8 / iter7 400/400 in requests, account state and
now also engine inputs (were 0/400 on `realized_pnl_cumsum_last`, D17.4),
`.local/fake_v8_fills/grid_v7` 150/150 requests / account state, engine
inputs 2/150 (148 differ only in `realized_pnl_cumsum_*` through the live
fills the harness never refetches). `cargo fmt --all --check`, clippy `-D
warnings`, `cargo test --workspace` (118 runner lib tests; new: 11
hsl_coin, 1 snapshot `hsl_coin_modes_override_steps_two_three_and_protective_input`,
1 live `realized_pnl_cumsum_applies_the_zero_fee_fallback_like_the_hsl_ledger`). Python facts found:
`_equity_hard_stop_refresh_coin_cooldown_after_repanic` is not bound on
`Passivbot` in v8.1.0 (hsl:4776; a repanic reset with the `panic` cooldown
policy would raise), and the fake harness runs the production coin
supervisor loop at one scenario minute (four iterations per red episode).

**Not modelled (D20):** replay-matrix cache (accelerator only),
background / partial replay, latch files, operator runtime forced modes,
coin overrides of `n_positions`.

**Next action:** P5.2 shadow run with an HSL-enabled config (coin mode
default) to see `initialize_coin_from_history` against a live fill
history; then the entry-cooldown position-delta guard.

## 2026-09-08 (session 4) — line 8 feature-complete for a shadow/small-capital run; all agent work merged on master

Summary of the day (details in the worktree-agent entries, newest first):
churn gate (D13), pre-create market gate + distance filter (D14),
SNAPSHOT_SPEC 8 gaps (D15), HSL account-level machine (D16) and coin mode
(D20), mock exchange + `pb-mockrun` closed loop (D17, P5.1 ticked), P7
survey (D18, deferred), two pre-live review passes and their fixes
(`docs/REVIEW_2026-09-08.md`, D19; second pass: failed creates enter the
recent-execution guard, an exposed symbol drop fails the cycle, hedge-mode
retry on rate limits, no barrier bypass for market panic closes), candle
refetch throttled to bucket boundaries, rate-limit back-off,
`realized_pnl_cumsum` fee fallback. pbtb-rust runtime selection: PR
https://github.com/iengai/pbtb-rust/pull/35 (D12; not merged, nothing
applied). P6.4 verified. Memory note: pushes to `iengai/*` need
`gh auth switch --user iengai`.

**Verified on master (HEAD of this entry):** `cargo test --workspace` 137;
plancheck 600/600 x2 + 400/400 x4; mockrun identical in requests, account
state and engine inputs on those six, fills run 150/150; snapcheck
identical on seven fixture sets (grid_v7, tm, grid_v7_seeded, tm_seeded,
grid_v7_forced 30/30 each; grid_v7_hsl 27/27 and grid_v7_hsl_coin 25/25
with every traced HSL state matching); `pb-runner --once` dry run vs the
abot account: 1391 fills / 514 closed-pnl rows over 30 days (Python
cross-check identical).

**Container soak (dry-run, read-only key, linux/amd64 image, 64 MiB cap):**
first build 257 cycles / 3 rate-limit errors in 20 min (20 kline requests
per cycle); after the throttle 2829 cycles over ~2 h with zero errors, RSS
20-23 MiB, planning p50 428 ms / p99 1.9 s / max 4.4 s; review-fix image
2062 cycles zero errors, RSS 22-26 MiB. Restarted on the final image of
the day (`pbr-soak`); check `docker logs pbr-soak` next session.

**Open gaps (all documented, none blocks a shadow run):** operator runtime
forced modes (no source in the runner); cached forager-metric fallback
(runner fails the cycle where Python ranks on stale metrics); HSL
replay-matrix cache / background replay / latch files (accelerators only);
`normalize_open_order` reduce-only rule from config `hedge_mode` (moot,
hedge mode always asserted); arm64 image never built (CodeBuild, user);
live execution never exercised with a trading key; SIGINT outside the
sleep windows is swallowed (dev only).

**Next action (autonomous):** keep the soak running and read its log at
the next session start; nothing else in PLAN P1-P6 is doable without the
user. If new fixtures are wanted: a fake run with a re-entry during an
HSL cooldown (repanic path is unit-tested only).
**Small-capital candidate config (user, 2026-09-08):** the v7-migrated
`bybit-cap300-iter1-winner-v810.json` (private, strategy_lab; long
`n_positions=1`, `twel=1.75`, short off, HSL off, unified mode). Verified:
`--check-only` ok; read-only dry run vs abot (8 symbols, 1397 fills, plan
0.75 s); a 400-step fake-exchange run with it (`.local/fake_v8_cap300`,
balance 300, one seeded position): diffcheck 400/400, snapcheck 400/400,
plancheck 400/400, mockrun engine inputs 400/400. Usable as the P5.3
config. PR #35 review from the user received (1 must-fix: RUNBOOK deploy
order lambda-before-`8rs`; 3 optional cleanups) — being applied.

**Rollout (user go-ahead 2026-09-08, RUNBOOK "pb-runner runtime"): steps 1-5
done.** PR #35 merged (5bd5afe). Step 1 ECR repo `pb-runner` created
(terraform, module.ecr only). Step 2 image `pb-runner:8-v8.1.0-arm64` built
locally (buildx/QEMU, 65 min) and pushed. Step 3 lambda-deploy run 34179407866
succeeded (new task-state-change-handler, CodeSha256 `8OJsua5g...`), getting
the `8rs`-aware binary live before the table gained the key. telebot-build
first failed on the EOL `bullseye-security` apt suite; the user merged the
bookworm fix (PR #36, d6a2139) and run 34184699943 pushed telebot `:latest` =
`d6a2139`. Step 4: `8rs` uncommented in `terraform/envs/dev/terraform.tfvars`
and applied scoped (`module.passivbot_task["8rs"]` + lambda + telebot
base-env) -- 2 added, 2 changed, 0 destroyed; task def
`scalable-cluster-dev-passivbot-v8-rs:1` (image `pb-runner:8-v8.1.0-arm64`,
memory 96, cpu 128, `--live`, log group
`/ecs/scalable-cluster-dev/passivbot-v8-rs`), lambda table now
`7=...:3,8=...-v8:1,8rs=...-v8-rs:1` with the function code untouched
(`source_code_hash` ignored by the module). The apply ran from worktree
`E:\projects\pbtb-rust-8rs` (branch `chore/enable-8rs-engine`, PR
https://github.com/iengai/pbtb-rust/pull/37) after copying the existing
`target/lambda/task_state_change_handler/bootstrap` in -- the lambda module's
`archive_file` needs it present, and the byte-identical copy keeps the S3
artifact out of the diff. Step 5: telebot-deploy run 34185540023 succeeded
(`tag=latest`, `passivbot_revisions=latest`), remote health `telebot-up`, the
composed table carries `8rs=...-v8-rs:1`.
**Needs the user:** merge PR #37 (live state now has `8rs` while `main` does
not -- an apply from `main` before the merge would plan to destroy the new
task def); then step 6 on Telegram (choose the cap300 config, `/runtime
<bot_id> rs`, Stop, Run, watch `/ecs/scalable-cluster-dev/passivbot-v8-rs`);
approve the small-capital live run details (sub-account, keys via S3). Memory
96 MiB is a placeholder -- measure after the first `rs` bot start (soak RSS
was 20-26 MiB) and lower it.

## 2026-09-08 (worktree agent) — pre-live review findings 1-8 fixed (fill windows, error budget + in-process restarts, market orders, lazy exchange config, dirty symbols)

**Changed:** `crates/exchange-bybit`: `fetch_fills` / `fetch_closed_pnl`
walk explicit 7-day `[startTime, endTime]` windows over the whole range
(`weekly_windows`, bybit.py:200-225 / 279-308 / 495-500), cursor pagination
per window, dedupe; `NewOrder.order_type` (`OrderType::{Limit,Market}`) and
`order_body` sends `orderType: Market` without `price`/`PostOnly` as ccxt
does; `cancel_orders` returns `CancelAck { already_gone }`;
`configure_symbol` = ccxt `set_margin_mode` (unified account ->
`/v5/account/set-margin-mode`, detected through `/v5/user/query-api` like
`is_unified_enabled`; classic -> `switch-isolated`) then `set-leverage`,
not-modified tolerated; `parse_markets` keeps non-Trading perpetuals with
`MarketSpec.active = false`. `crates/runner`: new `exchange_config.rs`
(`update_exchange_configs` port: lazy per create symbol, backoff, rate-limit
stop, 0.2 s pause, `min(live.leverage, market max)`, cross margin);
`execute.rs` (dirty symbols from failed/already-gone cancels skip the wave's
creates, lazy configuration before creates, market order type, one error
budget `note_error` shared with planning failures, length mismatch no longer
charged); `live.rs` (hourly `load_markets` + hedge-mode re-assert with
budget charge on failure, fills/closed-pnl refetched every cycle from an
hour before the last sync and pruned to the lookback, per-symbol
degradation: missing ticker / market / candles drop the symbol with its
open orders untouched, `active` -> `tradable`, approved coins need an active
market, `configure_exchange` = hedge mode always with `init_markets`' three
network retries); `main.rs` (Python `main()` lifecycle in-process: budget
trip or failed warmup -> teardown, 60 s cooldown, fresh bot, exit 30 after
`max_n_restarts_per_day`; `time_in_force` from `ConfigView`, default GTC);
`mock_exchange.rs` fills market orders at the step price as taker
(`fake.py:790-808`); `bin/mockrun.rs` follows. Docs: REVIEW resolutions per
finding, RECONCILE_SPEC 3.5.1, CONTRACT section 2 (restarts: pbtb-rust only
relaunches OOM stops), MOCK_EXCHANGE deviation 2, PLAN P4.4/P4.5, D19.

**Verified:** `cargo fmt --all --check`, `cargo clippy --workspace
--all-targets -D warnings`, `cargo test --workspace` (107 tests; new: 2
exchange-bybit (`weekly_windows`, market order body), 2 exchange_config, 4
execute, 1 reconcile, 1 mock_exchange). `pb-plancheck` unchanged: public
grid_v7 600/600 (both artifact dirs), tm 600/600; seeded2 grid_v7, tm, tm8,
iter7 400/400. `pb-mockrun --diff-inputs` identical requests / open-order
sets / positions / balances / fill counts on the same six (600/600 x3,
400/400 x4) and on `.local/fake_v8_fills/grid_v7` 150/150, engine-input
differences unchanged (only `realized_pnl_cumsum_*`, D17.4). `pb-snapcheck`
identical on grid_v7, tm, grid_v7_seeded, tm_seeded, grid_v7_forced 30/30
each. `pb-runner --once` dry run against the abot account: `fill history
loaded fills=1391 closed_pnl=514 lookback_days=30 oldest_fill_ms=1786318210248
newest_fill_ms=1788795142014` (the whole 30 days; before the fix only the
first 7 days loaded), `post_only=false`, cycle planned 4 ideal / 2 cancels
/ 2 creates, 0 skipped symbols. Python cross-check through ccxt with the
same key (`fetch_fills` + `fetch_pnls_sub` reproduced): 1391 fills in the
lookback (1393 with the 1 h overlap), 514 closed-pnl rows, same newest
fill, 5 `fetch_my_trades` calls.

**Not fixed (REVIEW 8):** `normalize_open_order` reduce-only rule from the
config's `hedge_mode` instead of the order's positionIdx; recent-execution
guard stamped at loop start. Both noted in D19.

**Next action:** HSL state machine (parallel worktree, `snapshot.rs`);
P5.2 shadow run; P6 image build with the new lifecycle.

## 2026-09-08 (worktree agent) — HSL equity hard stop ported (SNAPSHOT_SPEC 2.3 step 1, D16)

**Changed:** new `crates/runner/src/hsl.rs`: `HslConfig` (`_parse_hsl_config`
+ `live.hsl_signal_mode` / `hsl_position_during_cooldown_policy` /
`pnls_max_lookback_days` / `fee_pct_fallback`; refuses coin mode with HSL
enabled), `HslState` on top of the engine's `HardStopState` +
`RollingPeakTracker` (`apply_sample` with the per-minute cache,
`compute_stop_event`, `finalize_red_stop` via
`evaluate_red_episode_finalization`, `handle_position_during_cooldown`,
`check`, `supervise_red` (`Supervision::{Production, FakeHarness}`),
`sync_flat_finalize`, `modes`, `initialize_from_history`),
`balance_equity_timeline` (= `get_balance_equity_history` for the
account-level modes: fill replay, f32 closes, finalized minutes only),
`FeePolicy` (fill-manager fee normalisation), `HslModes` / `HslSideMode`;
16 unit tests mirroring `tests/test_equity_hard_stop*.py` cases.
`snapshot.rs`: `CycleState.hsl` + `known_symbols` (`set(self.positions)`),
`SnapshotBuilder::with_hsl`, `side_forced_mode` (= `get_forced_PB_mode`)
in `mode_override` step 1 and `_pside_blocks_new_entries`; `is_forager_mode`
now reads only the configured forced mode (pb:8243, was wrongly also HSL);
1h log-range EMA map is all-or-nothing (`fetch_required_map`); universe
keeps known symbols; unit test `hsl_side_modes_override_step_one_and_keep_known_symbols`.
`live.rs`: `LiveRunner.hsl`, `initialize_hsl` at warmup (fill history +
1m buffers), per-cycle `check` + red supervision before the snapshot,
`hsl_fills` / `hsl_positions` / `hsl_observation`. `bot_params.rs`:
`hsl_side`. `bin/snapcheck.rs`: per-recording `Checker`, HSL trace replay
(`--hsl-trace`, `--fills`, state assertions, derived-input cross-check with
the stale-ledger classification, mode overrides from the Rust machine).
Tools: `fake_live_clock.py` HSL trace wrapper (`PB_RUNNER_HSL_TRACE`),
`record_fake_v8.py` sets it and copies `fills.json`, `select_fixtures.py`
keeps trace/fills/scenario and HSL transitions. New fixtures
`tests/fixtures/configs/fake_v8/grid_v7_hsl.json` and
`tests/fixtures/recordings/fake_v8/grid_v7_hsl` (27 cycles + trace + fills +
scenario). Docs: SNAPSHOT_SPEC 2.3 / 8, PLAN P4.1 / P4.2, RECORDER A,
fixtures README, D16.

**Verified:** full HSL run (`.local/fake_v8_hsl/grid_v7_hsl`, boot
2025-10-10 20:00 into the Oct 10 crash, 398 cycles): `pb-snapcheck`
398/398 identical with the mode overrides produced by `hsl.rs`
(green 0-58, yellow 59-65, orange 66-72 = `tp_only`, red 73-74 = `panic`
closes, halted 75-191 = `graceful_stop`, reset 192+), "every traced state
matches" over 1 init / 397 checks / 4 supervisor steps / 1 finalization /
1 reset, 10822/10835 floats bit-exact (rest 2.7e-16, summation order), 646
realized-pnl inputs differ only because the harness never refreshes fills
after boot. Committed subset 27/27. Regression: `pb-snapcheck` grid_v7,
tm, grid_v7_seeded, tm_seeded, grid_v7_forced 30/30 each; `pb-plancheck`
public grid_v7 600/600, tm 600/600, seeded grid_v7 / tm / tm8 / iter7
400/400. `cargo fmt --check`, clippy `-D warnings`, `cargo test --workspace`
(84 runner lib tests; new: 16 hsl, 1 snapshot).

**Not modelled (D16):** HSL coin mode (config refused), panic-marker
reconstruction, the production protective-panic input path (red
supervision runs through normal planning with `panic` overrides), operator
runtime forced modes; `live::realized_pnl_cumsum` does not apply the
zero-fee fallback yet (SPEC 5.2 follow-up).

**Next action:** P5.2 shadow run with an HSL-enabled `unified` config to
see the runner's `initialize_hsl` against a live fill history; apply
`FeePolicy` to `realized_pnl_cumsum`; then the entry-cooldown
position-delta guard.

## 2026-09-08 (worktree agent) — P5.1 closed: mock exchange + `pb-mockrun`, six runs identical in requests and account state

**Changed:** new `crates/runner/src/mock_exchange.rs`: `Scenario` (scripted
`timeline` rows or candle `replay` from `.npy` day files / inline candles,
boot positions / fills / orders, ISO or epoch timestamps) and
`MockExchange: ExchangeClient` mirroring `src/exchanges/fake.py` operation
for operation (fill on the next candle's range or at creation when the
step price crosses, fees, balance, position netting per pside, order and
trade ids seeded by the boot fills, `fetch_open_orders` `(timestamp, id)`
string order, tickers = step price, `manual_fill` / `cancel_open_orders`
actions, request log); `fetch_ohlcv` follows the Bybit client's paging
contract instead of the fake's newest-`limit` quirk. 13 unit tests on the
fill model and the API. New `pb-mockrun` (`bin/mockrun.rs`): `LiveRunner`
+ `Executor` (the `--live` path) drive the mock through a
`tools/record_fake_v8.py` run directory with the harness pacing (one wave
per step, then `advance`; wall clock = scenario time, churn clock = the
recording stem per D13), comparing per step the create/cancel requests
(content, order/id sequence), the open-order set (content and ids),
positions, balance and fill count against `remote_calls.json`,
`step_summaries.json`, `fills.json`, and the final
`fake_exchange_state.json`; `--diff-inputs` diffs the engine input against
the recording. `live.rs` (minimal, separated): `LiveRunner::with_clocks`
(injected wall/monotonic clocks, `new` unchanged in behaviour),
`set_harness_secondary_never_fetched` (harness-only flag feeding
`candles_available`), and the first-minute trailing rule (side
unavailable until a full minute closed after the last fill, Python's
`missing_exact_trailing_candles`). Docs: MOCK_EXCHANGE.md (line-by-line
map to `fake.py`, deviations, results), D17, PLAN P5.1 ticked, README.

**Verified:** `pb-mockrun --diff-inputs`: public grid_v7 600/600 (both
artifact dirs), tm 600/600, seeded2 grid_v7 400/400, tm 400/400, tm8
400/400 (no order in either bot), iter7 400/400 — identical create and
cancel request sets, identical create order/id sequences and cancel id
sets, identical open-order sets and ids, positions, balances and fill
counts at every step, final state identical, 0 planning errors, 0 write
failures. Engine inputs identical 600/600 on both public runs; on the
four seeded runs the only differing field is
`global.realized_pnl_cumsum_last` (`0.0` vs `-0.0530015256`: Python's
`fee_pct_fallback` on the fee-less seeded boot fills, D17.4, no order
affected). Control runs fail as they should: `--no-harness-compat` on
public grid_v7 267/600 requests, open-order set 43/600 (the runner rotates
forager entries the harnessed Python bot could not, D17); `--gate-clock
cycle` on iter7 398/400 requests, open-order set 367/400 (D13).
Extra run with fills (`.local/fake_v8_fills/grid_v7`, grid_v7, 150 steps, `--seed-positions 3 --seed-entry-offset -0.03`, positions 3 % in profit): the close-grid orders cross at creation at step 1 (3 maker fills: ADA 233 @ 0.6425 pnl 4.423272, BTC 0.001 @ 110050 pnl 3.31217, DOGE 769 @ 0.19482 pnl 4.434823, fees 0.0150/0.0110/0.0150), positions go flat, fresh initial entries follow; `pb-mockrun --diff-inputs` 150/150 identical requests, open-order sets and ids, positions, fill counts and balances (final 1012.129308092 bit-identical), final state identical; engine inputs differ only in `realized_pnl_cumsum_{last,max}`: Python's series stays at the boot-fill fee fallback (-0.079479763 / 0.0) because the harness primes the fill cache once and the bot never calls `fetch_my_trades` (zero such calls in every run's `remote_calls.json`), while the runner refetches fills and its series reaches 12.129308092; no order affected (flat positions, no unstuck).
`cargo fmt --all --check`, `cargo clippy --workspace --all-targets -D
warnings`, `cargo test --workspace` (98 tests; new: 13 mock_exchange).

**Limits:** none of the six recorded runs produced a live fill, so their
balance/position parity is trivial; the fill model rests on the unit tests
and the extra fills run above. The mock cannot exercise market orders,
partial fills or exchange errors (the fake exchange does not model them
either). Not ported: `fee_pct_fallback` on fee-less fills.

**Next action:** HSL equity state machine (in flight in a parallel
worktree, `snapshot.rs`); P5.2 shadow run against the abot account; P6.

## 2026-09-08 (worktree agent) — SNAPSHOT_SPEC 8 gaps closed except HSL

**Changed:** `crates/runner/src/snapshot.rs`: `CycleState` (previous
`PB_modes`, dynamic forager eligibility, close-EMA carry-forward cache,
cooled symbols) passed as `&mut` to `build`; `Snapshot.mode_overrides` and
`pb_modes_after_cycle` (`_python_mode_from_orchestrator_state`); verbatim
ports of `normal_planning_psides`, `dynamic_forager_normal_psides`,
`dynamic_forager_managed_entry_psides`, `flat_forager_default_normal`,
`candidate_only`, `required_ema_can_mark_nontradable`; the missing
required-forager rule now raises on `!can_mark_nontradable` (was
"priority") plus the cache-only rule; exchange-cooldown planning policy
(`cooldown_mode`) and flat-symbol tradability; close-EMA carry-forward
(`close_ema_fallback_max_age_ms`) and open-tail projection wiring.
`crates/runner/src/emas.rs`: `open_tail_gap`, `open_tail_rows`,
`projected_ema` (= `cm.get_projected_open_tail_ema_metrics`).
New `crates/runner/src/cooldown.rs` (`ExchangeCooldowns`, config
validation, Bybit classifier = `None` as in v8.1.0). `execute.rs`
`WaveReport.write_failures`; `live.rs` owns `CycleState` +
`ExchangeCooldowns`, `note_write_failures` (called from `main.rs`);
`pb-snapcheck` replays `PB_modes` from the previous recording's output.
Tools: `record_fake_v8.py` decodes the harness output as UTF-8 (cp932
crash after a complete run, RECORDER pitfall 6). New fixture set
`tests/fixtures/recordings/fake_v8/grid_v7_forced` (30 cycles) from
`tests/fixtures/configs/fake_v8/grid_v7_forced.json` (grid_v7 +
`coin_overrides.{ADA,BTC,DOGE}.live.forced_mode_long` = gs / tp_only / m,
`--seed-positions 3`). Docs: SNAPSHOT_SPEC 2.3, 3.6, 8; PLAN P4.1/P4.2;
RECORDER section A; D15.

**Verified:** `pb-snapcheck` identical on grid_v7 30/30, grid_v7_seeded
30/30, tm 30/30, tm_seeded 30/30, grid_v7_forced 30/30 (full local run
400/400: ADA graceful_stop = 399 closes + 376 grid re-entries and no
initials, BTC tp_only = closes only, DOGE manual = no orders), and on the
full 600-cycle public runs grid_v7 600/600, tm 600/600. `pb-plancheck`
unchanged: public grid_v7 600/600, tm 600/600; seeded grid_v7, tm, tm8,
iter7 400/400 each. `cargo fmt --check`, clippy `-D warnings`,
`cargo test --workspace` (71 tests; new: 3 cooldown, 2 emas, 7 snapshot).
Evidence for (d): the fake harness primes every coin's full 1m array each
step, so the last closed minute is never missing; neither the carry-forward
nor the projection fires in any fake run (600/600 with and without the
code). Their behaviour is covered by unit tests mirroring pb:18687-18830
and cm:9221-9400.

**Not modelled (D15):** HSL modes (separate task), runtime operator forced
modes, `ineligible_symbols`, cached forager-metric fallback and forager
stale-tail context (runner skips the cycle with an error where Python
would rank on stale metrics), EMA-entry-cancellation order keys,
entry-cooldown position-delta guard.

**Next action:** HSL equity state machine (SPEC 2.3 steps 1-2, D15 item 5),
then the P5.2 shadow run; the market-distance filter is in flight in a
parallel worktree (`reconcile.rs`/`live.rs`).

## 2026-09-08 (worktree agent) — pre-create market snapshot gate + distance filter ported (RECONCILE_SPEC 2.10)

**Changed:** new `crates/runner/src/market_filter.rs`: `MarketSnapshot`
(bid/ask/last + local `fetched_ms`), `SnapshotProvider` (Python
`MarketSnapshotProvider`, bulk strategy: cache within `max_age_ms`, one bulk
`fetch_tickers`, one retry fetch, `Incomplete`/`Fetch` errors),
`planning_snapshot_invalid`, `snapshot_signature_invalid`,
`MarketFilter::{from_config, filter_by_market_distance, filter_fresh_creations}`
with Python's log lines and the hourly INFO throttle; 13 unit tests
mirroring `tests/test_passivbot_balance_split.py` and
`tests/test_fresh_entry_eligibility_integration.py`. `OrderRec.market_distance`
(`_churn_gate_market_distance`). `reconcile()` lost its churn argument and
ends at the recent-execution guard; new `reconcile::admit_and_cap` = churn
admission (reading `market_distance`) + create capacity + attempt
bookkeeping. `LiveRunner` owns the snapshot cache: planning tickers come
from `SnapshotProvider::get_snapshots` with the 5 s fetch TTL, the
pre-create gate re-reads it with the 10 s hard TTL, then `admit_and_cap`.
`pb-plancheck` applies the gate + distance filter per step on the recorded
price and prints the skip count. `Plan.skipped_market_snapshot` /
`skipped_market_distance`, logged by `pb-runner` as `skipped`.
SPEC: new 2.10, 4.3 item moved to "ported", section 5 item 3 resolved;
PLAN P4.3 note; D14.

**Facts:** max age is the constant 10 000 ms (`md.py:656`), not config;
freshness compares `utc_ms()` at the check against the *local receive time*
of the fetch; the Bybit connector drops the ccxt ticker `timestamp`
(`ccxt_bot.py:1219`), so no ticker timestamp is needed in the Rust client.
Python's call order is barrier/guards -> market filter -> churn admission ->
capacity (`exe.py:958-975`); the churn admission takes its market distance
from the filter. Whole-cycle skips drop market orders too; the distance
filter exempts them and symbols without a valid snapshot; `t == 0` disables
the skip but still annotates.

**Verified:** `pb-plancheck` grid_v7 600/600 (both artifact dirs), tm
600/600, seeded2 grid_v7 400/400, tm 400/400, tm8 400/400, iter7 400/400,
all with 0 market-filter skips (the fake ticker is `bid=ask=last=price`
and `utc_ms` is pinned, so snapshots are never stale; no
`far-from-market` / `skipping order creation` line in any `fake_live.log`,
no `create_skipped` event in any `live_events.json`). `cargo fmt --check`,
clippy `-D warnings`, `cargo test --workspace`. Not exercised: a real stale
or failed ticker refresh on the live account (dry-run only).

**Next action:** unchanged: (2) HSL / cooldown / runtime-forced modes with
a seeded fake run; (3) long local dry-run of the container against the abot
account; P5.2 shadow run, P6. Optional: a fake scenario with a price jump
> 80 % between steps to exercise the distance skip end to end.

## 2026-09-07 (worktree agent) — order churn gate ported (RECONCILE_SPEC 2.9)

**Changed:** `crates/runner/src/churn.rs` (`ChurnParams::from_config`,
`ChurnGate`: evidence `evaluate`, admission `admit`, `record_attempts`,
`monotonic_seconds`; 20 unit tests mirroring `tests/test_order_churn_gate.py`
and the admission arithmetic). `OrderRec.churn_evidenced`; `reconcile()`
takes `Option<(&mut ChurnGate, f64)>`: admission runs after the
recent-execution guard and before the creation capacity, and the creates
left in the plan are recorded as attempts (exempt ones too, as Python does
for every submitted create; `execute.rs` submits `plan.creates` as-is).
`LiveRunner` owns the gate, evaluates the executable ideals before
reconciliation and derives the risk-phase pairs (`risk_active_pairs`:
risk-critical orders + `loss_gate_blocks`). `pb-plancheck` runs the gate per
step in order (`--gate-clock wall|cycle`, `--churn-trace`).

**Verified:** `pb-plancheck` grid_v7 600/600, tm 600/600, seeded grid_v7
400/400, tm 400/400, tm8 400/400, iter7 400/400 (was 367/400). Python's
own `order.churn_evidence` reason counts and `order.churn_admission`
rolling counts (the last 2000 events each run kept) equal the Rust trace
cycle for cycle: iter7 50/50 evidence + 36/36 rolling, tm8 60/60,
grid_v7 52/52 + 14/14. `cargo fmt`, clippy `-D warnings`,
`cargo test --workspace`.

**Fact (D13):** the fake harness does not pin `time.monotonic()`, so the
Python gate ran on wall-clock time in every recorded run (~0.8 s per step,
`rolling_usage=90` at the first deferral). plancheck feeds the recording
stem's wall-clock ms as the gate clock; `--gate-clock cycle` (60 s per step)
gives the old 367/400 on iter7.

**Next action:** unchanged (P5.2 shadow run, P6); the market-distance
filter (SPEC 4.3) is still open.

## 2026-09-07 (session 3) — P2 recordings, Bybit client, snapshot builder, reconcile, dry-run loop, image

Long entry; the "Next action" block at its end is the resume point.

**Mandate (user, this session):** advance until a pure-Rust bot can be
deployed on pbtb-rust; open decisions go to a same-tier subagent for review;
no upstream PR for the rlib plumbing; repo is public and stays so
(memory: pb-runner-mandate). History was rewritten to remove the company
account name; origin/master = 26b3789 + this session's commits.

**Done so far:**
- Recorder patch applied (uncommitted) to `E:\projects\passivbot-rlib-v8.1.0\src\passivbot.py`.
- Tools: `tools/record_fake_v8.py` (replay scenario + harness driver + MANIFEST,
  `--seed-positions`), `tools/fake_live_clock.py` (pins all clocks to fake
  time, ccxt-like `fetch_ohlcv` paging, vectorised candle priming),
  `tools/select_fixtures.py` (subsample), `tools/make_public_configs.py`
  -> `tests/fixtures/configs/fake_v8/{grid_v7,tm}.json`. Details and the
  five pitfalls in RECORDER.md section A. Decision D10 (public configs for
  committed fixtures; private recordings stay in gitignored `.local/`).
- Private recordings (600 cycles each, 2025-08-01..10-28 replay, boot day 84):
  iter7, iter12, tm8 -> diffcheck 1800/1800 ok. Only
  `entry_initial_normal_long` orders appeared (no fills in 10 h of replay).
- Detached jobs running (`.local/fake_v8/run_*.sh|log`): public configs
  (grid_v7, tm; 600 cycles) then seeded batch (grid_v7, tm, iter7, tm8;
  400 cycles, 2 seeded long positions 2% under water).

**Next:** when jobs finish: diffcheck each set; `select_fixtures.py` public
sets (stride 20, max 60) + seeded public sets into
`tests/fixtures/recordings/fake_v8/{grid_v7,tm,grid_v7_seeded,tm_seeded}`;
diffcheck committed sets; tick P1.2/P2.1/P2.2; commit+push. Then P3.1:
ccxt HEAD on 2026-09-07 = `11f45ee2bf0d2f809c318761c717415268da27c0`;
rust/ tree = workspace {ccxt, ccxt-base, ccxt-pro, ccxt-prediction, tests}, 56 MB.
P3.1 facts: `transpiled-base` compiles all 209 exchanges (no per-exchange
feature); dev build of ccxt-base+ccxt+ccxt-pro = 7 min 22 s on 32 threads,
debug target 12 GB, ccxt-base rlib 2.5 GB. Typed Bybit API covers every call
the Python bot uses (`&mut self`, `crate::Result<T>`; set_leverage /
set_position_mode / set_margin_mode are untyped core calls; errors carry the
ccxt kind string). Python-side semantics recorded in PORT_INVENTORY section 3.
Adjudication A (full ccxt dep) / B (hand-written v5 client) / C (vendored
bybit slice) delegated to a subagent (brief in scratchpad `p3_brief.md`);
verdict recorded as D11 (hand-written client).
P3.2 done: `crates/exchange-bybit` = signing, envelope/error classes,
parsers (numbers via `str::parse`), `ExchangeClient` impl; 15 unit tests.
P3.4 done: `examples/readonly_probe.rs` and `tools/probe_python_ccxt.py`
(ccxt 4.5.66, the version passivbot pins) agree on every stable field for
the abot account (`tools/compare_probes.py`). Two parity facts learned and
encoded: ccxt 4.5.66 gives `limits.cost.min = None` for Bybit linear so
passivbot's `min_cost` is always 0.1 (`or 0.1`); fee rates are ccxt's
describe defaults 0.0001 / 0.0006, not 0.0002 / 0.00055. Rust probe ~0.9 s
vs Python ~3.7 s for the same 7 calls. P3.3 (fills) is implemented as
`fetch_fills` (`/v5/execution/list`); private WS deferred.
`docs/SNAPSHOT_SPEC.md` (893 lines, subagent) is the P4.2 field-by-field spec.
P4.2 started: `crates/runner/src/bot_params.rs` (`ConfigView`) reproduces
`global_bot_params`, per-symbol `bot_params` and `strategy_params` exactly
(JSON Value equality, int vs float preserved) for every committed grid_v7
and tm fixture recording. Facts encoded: hjson parses integral literals as
ints (`0.0` -> `0`), the loader fills missing keys from the v8.1.0 template
(`crates/runner/assets/template_v8.1.0.json`), forager weights are
normalised at load, `wallet_exposure_limit = round(twel/n_positions, 8)`.
P4.2 snapshot builder done for the fake sets: `emas.rs` (window = ceil(span)
closed buckets, candle fields rounded to float32 like `CANDLE_DTYPE`, engine
`ema_last_f64`, provisional/strict gap policies, 1h aggregation) and
`snapshot.rs` (universe, modes steps 4-7, spans per strategy, tradability
with the forager cache-only rule, peek hints, incumbents, global). Acceptance
tool `pb-snapcheck` (parses recordings with correctly-rounded floats, D8;
replays the fake exchange's timeline gap fill): grid_v7 30/30, tm 30/30,
grid_v7_seeded 30/30 identical. Gaps to close before P5 (SPEC section 8):
HSL/cooldown/runtime-forced modes, `PB_modes` carry-over for tradability,
close-EMA 10-min carry-forward, open-tail projection, trailing from real
fills, entry-cooldown fill timestamps, realized-pnl cumsum from fills.
Fixture sets now: grid_v7, tm (unseeded), grid_v7_seeded, tm_seeded (boot
positions + boot fills: closes, grid entries, cropped entries); snapcheck
identical on all 120 recordings (trailing bundles from fill anchors with
float32 candles, `is_trailing` rule). Live loop skeleton `live.rs` +
`pb-runner --dry-run --once` verified on the abot account (read-only key):
one cycle 4.3 s. `docs/RECONCILE_SPEC.md` (819 lines, subagent) written;
P4.3 `reconcile.rs` and P4.4 `execute.rs` implement its "minimal faithful
subset"; `live.rs` keeps `PB_modes`, closed-pnl history, realized-pnl
cumsum and entry-cooldown timestamps; `startup.rs` downloads config/keys
from S3 with the contract's exit codes. `pb-runner --dry-run --once` on the
abot account now prints the reconciled plan (cancels of the live v7 bot's
XRP orders + 2 entries, expected with a different config). All four
seeded-with-fills private sets: 400/400 diffcheck.
`pb-plancheck` (P5.1-lite): the reconcile plan equals the Python bot's
actual create/cancel requests on 2400/2400 cycles of five full runs; the
sixth (seeded iter7) differs on 33/400 cycles where Python's order churn
gate deferred far grid entries. `jsonexact` module = exact float parser for
recordings. Churn gate ported by a subagent and merged (entry above, D13):
all six local runs 2800/2800 identical plans.
P6: `docker/Dockerfile` + `deploy/buildspec.yml`; the same Dockerfile built
locally for linux/amd64 (63.2 MB image, 1m49s cold build); dry-run loop in
the container against the abot account, grid_v7 config, 10 symbols, 22
cycles: 19-20 MiB RSS (Python bot ~430 MB). Release binary 16.9 MB on
Windows. pbtb-rust: D7 resolved by D12 (subagent-written branch
`feat/pb-runner-runtime`, PR https://github.com/iengai/pbtb-rust/pull/35,
reviewed here: engine keys `<major>[rs]`, bot attribute `runtime`,
`/runtime` command, ECR repo `pb-runner`, `8rs` entry commented out until
the image exists; not merged, nothing applied).

**Not done / gaps:** market-distance filter (RECONCILE_SPEC 4.3); HSL,
cooldown and runtime-forced modes, close-EMA carry-forward, open-tail
projection (SNAPSHOT_SPEC 8); live execution never exercised with a trading
key; arm64 image never built (CodeBuild); P5.2 shadow run in ECS; P6.4
write-back check; P7 (v7 line). The passivbot worktree
`E:\projects\passivbot-rlib-v8.1.0` still carries the uncommitted recorder
patch and the Windows fcntl patch (keep them out of the fork branch).

**Next action (autonomous):** P5.2-prep and remaining SNAPSHOT_SPEC gaps
in this order: (1) market-distance filter + pre-create snapshot freshness
(SPEC 3.1 step 8) so the runner is safe with real money; (2) HSL /
cooldown / runtime-forced modes with a seeded fake run that exercises
them; (3) a long local dry-run of the container against the abot account
(hours) to catch drift, error-budget and reconnect behaviour.
**Needs the user:** merge PR #35 and apply Terraform (ECR repo, later the
`8rs` task definition); create the pb-runner CodeBuild project and run the
first arm64 build (P6.2); approve the ECS shadow task (P2.4/P5.2) and the
small-capital live run on a separate sub-account (P5.3).

## 2026-09-07 (session 2) — P1 done except the P2-gated box

**Remote (added later the same day):** `origin` = public repo
`https://github.com/iengai/pb-runner`, branch `master`. All commits are
authored as the private identity (AGENTS.md "Git identity").

**State:** P1.1 done and pushed (`iengai/passivbot` branch
`pb-runner/rlib-v8.1.0`, commit `e808cfd33` = tag v8.1.0 + build plumbing).
P1.2 implemented; the `passivbot_rust` git dependency is enabled in
`Cargo.toml` (locked to `e808cfd33`), `diffcheck --features engine` replays
recordings. P1.3 decided (D9). 153 synthetic recordings committed under
`tests/fixtures/recordings/synthetic_v8/` with `MANIFEST.json`.

**Verified this session:**
- Engine crate on the branch: `cargo build --no-default-features`,
  `cargo test --no-default-features` (254 passed), `cargo build` (default),
  `cargo fmt --check`, `pip wheel . --no-deps`, and
  `pytest tests/test_coin_filtering.py tests/test_candle_interval.py`
  (29 passed with the rebuilt extension; the same two files show 3
  "extension appears stale" failures in the *main* checkout because its root
  `passivbot_rust.pyd` has no fingerprint stamp: pre-existing, not ours).
- pb-runner gates: fmt, clippy `-D warnings` (default and `--features engine`),
  `cargo test --workspace`.
- `diffcheck --features engine --dir tests/fixtures/recordings/synthetic_v8`:
  153 ok / 0 failed, identical in dev and release (lto thin, cgu 1) profiles.
  Negative test: corrupting a recorded float by one ulp is reported with the
  differing byte offset.

**Facts gathered (see D8, D9, RECORDER.md "Pitfalls"):**
- serde_json best-effort float parsing caused 7 false mismatches; fixed by
  byte-exact text comparison. Rule: serde_json stays at the wheel's version
  (1.0.151), no `float_roundtrip`.
- The five pre-compute input validators live in `python.rs` (feature-gated);
  the runner must replicate them at P4.3.
- Dev-box layout: passivbot venv is Python 3.12 at
  `E:\projects\passivbot\.venv` (system `python` is 3.14, unusable for pyo3
  0.21); the site-packages `passivbot_rust` there is a stale build. The P1.1
  worktree `E:\projects\passivbot-rlib-v8.1.0` has the fresh extension in
  `src/` plus the uncommitted Windows `fcntl` patch (keep it uncommitted).
  Remote `iengai` was added to `E:\projects\passivbot`.

**Next action:** P2.1/P2.2: produce fake-exchange recordings from the
worktree (apply the RECORDER.md patch to its `src/passivbot.py` locally, or
reuse the plugin's wrapper), for the three configs named in PLAN P2.2, into
`tests/fixtures/recordings/fake_v8/`; run diffcheck on them; then tick P1.2
and P2.2. Probe done: the fake exchange is selected by
`live.fake_scenario_path` (`src/exchanges/fake.py:196`); scenario examples
are in `tests/test_run_fake_live.py` and `tests/test_fake_exchange.py`; the
configs are in the main checkout `E:\projects\passivbot\strategy_lab\configs\`
(`cap1000_iter7_highreturn.json`, `cap1000_iter12_alt_balanced.json`, and
`cap1000_iter8_tm_regime26.json` for trailing_martingale), not in the
worktree.

**Open questions for the user:** unchanged (D7; approval for P2.4 and P5.3).
Optional: open the upstream PR for the rlib plumbing (P1.1 last bullet).

## 2026-09-07 — P0 skeleton created

**State:** P0 done. No upstream dependency is enabled yet (both `passivbot_rust`
and `ccxt` git deps are commented in `Cargo.toml`). Nothing has been pushed;
the repo is local at `E:\projects\pb-runner` with one commit.

**Verified:** `cargo build --workspace`, `cargo test --workspace`,
`cargo run -p pb-runner -- <v8 config>` accepts a v8.1.0 config and refuses a
v7 one; `diffcheck` exits 2 on the empty fixtures dir.

**Facts gathered this session (sources in DECISIONS/CONTRACT):**
- pyo3 coupling in the engine crate is confined to `python.rs` (106 refs),
  `coin_selection.rs` (16), `utils.rs` (5), `lib.rs` (5), `types.rs` (4);
  `orchestrator.rs` and all strategy modules have none.
- Both v7.12.0 and v8.1.0 expose `compute_ideal_orders_json` /
  `OrchestratorInput`.
- ccxt official Rust port merged 2026-09-03 (bybit included, git-dep only).
- barter-rs has no Bybit execution client (mock + Binance placeholder only).
- pbtb-rust container contract: env `BUCKET/USER_ID/BOT_ID`, entrypoint
  downloads config + api-keys from S3, exec `python src/main.py configs/$BOT_ID.json`.

**Next action:** P1.1 — create the rlib feature branch of `passivbot-rust`
at tag v8.1.0 in the `iengai/passivbot` fork (see PLAN.md for the exact
edits and acceptance checks). Do it in a separate worktree of
`E:\projects\passivbot`, not on `v8.1.0-eval`.

**Open questions for the user (not blocking P1-P2):**
- D7 runtime selection in pbtb-rust (needed at P6).
- Approval for P2.4 shadow recording task and P5.3 small-capital run.
