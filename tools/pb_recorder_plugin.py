"""pytest plugin: record every passivbot_rust.compute_ideal_orders_json call.

Same on-disk format as docs/RECORDER.md; calls that raise are discarded
(synthetic fixtures must always have an .out.json).
"""
import hashlib, json, os, subprocess, sys, time
# Load the extension from the checkout's src/ (freshly built), never from
# site-packages: pytest plugins import before tests/conftest.py fixes sys.path.
_root = os.getcwd()
sys.path.insert(0, os.path.join(_root, "src"))
import passivbot_rust as pbr

_dir = os.environ["PB_RUNNER_RECORD_DIR"]
os.makedirs(_dir, exist_ok=True)
_orig = pbr.compute_ideal_orders_json
_count = {"ok": 0, "err": 0}

def _rec(input_json: str) -> str:
    stem = "%d_%s" % (int(time.time() * 1000), hashlib.sha256(input_json.encode("utf-8")).hexdigest()[:16])
    in_path = os.path.join(_dir, stem + ".in.json")
    with open(in_path, "w", encoding="utf-8") as f:
        f.write(input_json)
    try:
        out = _orig(input_json)
    except BaseException:
        os.remove(in_path)
        _count["err"] += 1
        raise
    with open(os.path.join(_dir, stem + ".out.json"), "w", encoding="utf-8") as f:
        f.write(out)
    _count["ok"] += 1
    return out

pbr.compute_ideal_orders_json = _rec

def _git(*args):
    return subprocess.check_output(["git", *args], cwd=_root, text=True).strip()

with open(os.path.join(_dir, "MANIFEST.json"), "w", encoding="utf-8") as f:
    json.dump({
        "producer": "pytest + pb_recorder_plugin (passivbot's own orchestrator tests)",
        "passivbot_commit": _git("rev-parse", "HEAD"),
        "passivbot_describe": _git("describe", "--tags", "--always"),
        "extension_path": pbr.__file__,
        "runtime_build_info": pbr.runtime_build_info(),
        "python": sys.version.split()[0],
        "recorded_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }, f, indent=2)

def pytest_sessionfinish(session, exitstatus):
    print(f"\n[pb-recorder] recorded ok={_count['ok']} discarded(raised)={_count['err']} -> {_dir}")
