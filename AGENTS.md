# AGENTS.md — working rules for pb-runner

This file is for AI agents (Claude Code etc.) and humans continuing the work
in a fresh session with no conversation history. Everything needed to resume
lives in `docs/`; nothing lives only in chat.

## Session protocol

1. Read `docs/STATUS.md` (current state, next action), then the phase in
   `docs/PLAN.md` it points at.
2. Work the phase. Tick checkboxes in `docs/PLAN.md` as items are verified,
   not when they are written.
3. Before ending: append a dated entry to `docs/STATUS.md` (what changed, what
   was verified, exact next action), and commit. A session that leaves
   `STATUS.md` stale has not finished.
4. New facts that change a decision go into `docs/DECISIONS.md` as a new
   entry (never silently edit an old one).
5. Reply to the user in Chinese. Docs and code comments in English.

## Map of sibling repositories (local paths on the dev machine)

| Path | What | Notes |
|---|---|---|
| `E:\projects\passivbot` | passivbot **v8.1.0** checkout, branch `v8.1.0-eval` at tag `v8.1.0` (commit `7af64f3e9`) | Upstream `enarjord/passivbot`. Has 3 uncommitted Windows patches (fcntl guard). Moves by tags only, never `git pull` master. `strategy_lab/` is gitignored research. |
| `E:\projects\pb-v712` | passivbot **v7.12.0** checkout (commit `fc6b9e016`) | Matches the live v7 ECS image. Do not force-rebuild its Rust extension. |
| `E:\projects\pbtb-rust` | Control plane (`iengai/pbtb-rust`) | Telegram bot, ECS launch, Lambda, Terraform. Routes bots to a task-def family by `config_version` major (`domain/engine.rs`, `terraform/envs/dev/terraform.tfvars` `passivbot_engines`). Container contract in `deploy/passivbot-image/`. |
| `E:\projects\rs-bybit` | third-party Bybit v5 Rust SDK (1 star, unmaintained) | Reference only for signing / WS auth shape. Do not depend on it. |

Engine crate lives at `<passivbot checkout>/passivbot-rust/` in each line.
Both lines expose `compute_ideal_orders_json` and `orchestrator::OrchestratorInput`.

## Hard rules

- **Never** put trading API keys on the dev machine or in this repo. The only
  local key is the read-only abot key in `E:\projects\passivbot\api-keys.json`
  (entry `415196485`); S3 per-bot keys and config-embedded keys are
  IP-whitelisted trading keys and must not be used locally.
- **Never** deploy, restart, or stop live bots, apply Terraform, upload to the
  config S3 bucket, or start CodeBuild without an explicit instruction in the
  current session.
- **Never** modify strategy logic in `passivbot_rust`. The only allowed change
  to the engine is build plumbing (make it usable as an rlib), on a branch
  named `pb-runner/rlib-<tag>` in the `iengai/passivbot` fork.
- Pin, do not track: passivbot by tag, ccxt Rust port by commit. Upgrades
  follow the procedure in `docs/PLAN.md` ("Upgrade procedure").
- GitHub: `iengai` is this machine's default gh account (2026-09-08). Leave it
  active -- the old rule to switch back to the company account after each push
  is retired. Merging your own PRs, pushing straight to `master` and running
  the Actions workflows are all delegated; the quality gates below still hold,
  and so does everything under "Never" above.
- Git identity: this project and every sibling repo listed above are
  private. Commits must be authored as `kk <iamibe.kai@gmail.com>` (the
  `iengai` identity). Since 2026-09-08 that is the machine's global git config
  as well as every repo's local one, so a fresh clone needs no setup; still
  worth `git log -1 --format='%an <%ae>'` after the first commit, which is the
  check that caught four commits going in under the company account.
- AWS: `AWS_PROFILE=dev`; in Git Bash set `MSYS_NO_PATHCONV=1` for `aws logs`.
- Windows dev box: `cargo` 1.90 on host works for build/test. arm64 images are
  built through pbtb-rust's CodeBuild pipeline (user-triggered), or locally
  with `docker buildx` if available.
- Long-running commands: the Bash tool caps background jobs at 10 min; launch
  longer jobs detached (`powershell Start-Process bash.exe <script>`).

## Quality gates

`cargo fmt --check`, `cargo clippy --workspace --all-targets -D warnings`,
`cargo test --workspace` must pass before a commit. `diffcheck` must report
0 failures on the committed fixtures before any change to snapshot
construction or reconciliation is merged.
