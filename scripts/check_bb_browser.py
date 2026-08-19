#!/usr/bin/env python3
"""
bb-browser 可用性探测 + 自动重启脚本（news-hotspots 技能本地版）

职责：
- 用 pipeline 中实际使用的 bb-browser 命令进行轻量探测
- 若全部失败，在本进程内执行重启（inlined browser-restart 逻辑，不跨技能调用）
- 重启完成后直接返回成功，不再次探测（重启后 bb-browser 是否真的可用，
  由后续各 fetch 步骤自行验证）
- 整体 timeout 由调用方控制（run-pipeline.py 设为 600s 足够）

与 browser-restart 技能的 restart_browser.py 完全等效，仅路径常量不同：
- Chrome user-data-dir: /home/clawd/.cache/chrome（固定）
- CDP 端口: 9222
- DISPLAY: :0
"""

import json
import os
import signal
import subprocess
import sys
import time
import urllib.request
import urllib.error
from pathlib import Path
from typing import Any, Dict, List, Optional

# ------------------------------------------------------------------
# 固定常量（与 browser-restart 技能保持一致）
# ------------------------------------------------------------------
FIXED_USER_DATA_DIR = "/home/clawd/.cache/chrome"
FIXED_CDP_PORT = 9222
DEFAULT_DISPLAY = ":0"
DEFAULT_CHROME_BIN = "google-chrome-stable"
DEFAULT_ZHIHU_URL = "https://www.zhihu.com/hot"
CHROME_LOG = "/tmp/news-hotspots-chrome-restart.log"
MANAGED_BROWSER_DIR = Path.home() / ".bb-browser" / "browser"
MANAGED_PORT_FILE = MANAGED_BROWSER_DIR / "cdp-port"

# ------------------------------------------------------------------
# 探测命令集（从各 fetch 脚本的实际调用中抽取）
# ------------------------------------------------------------------
PROBE_COMMANDS: List[Dict[str, Any]] = [
    {
        "name": "zhihu_hot",
        "cmd": ["bb-browser", "site", "zhihu/hot"],
        "timeout": 20,
        "expect_key": "items",
    },
    {
        "name": "google_news",
        "cmd": ["bb-browser", "site", "google/news", "OpenAI"],
        "timeout": 20,
        "expect_key": "articles",
    },
    {
        "name": "twitter_search",
        "cmd": ["bb-browser", "site", "twitter/search", "AI"],
        "timeout": 25,
        "expect_key": "tweets",
    },
    {
        "name": "weibo_hot",
        "cmd": ["bb-browser", "site", "weibo/hot"],
        "timeout": 20,
        "expect_key": "items",
    },
    {
        "name": "toutiao_hot",
        "cmd": ["bb-browser", "site", "toutiao/hot"],
        "timeout": 20,
        "expect_key": "items",
    },
]

RESULT: Dict[str, Any] = {
    "probe_results": {},
    "bb_browser_ok": False,
    "restart_attempted": False,
    "restart_success": False,
    "final_ok": False,
    "message": "",
}


# ------------------------------------------------------------------
# 辅助函数
# ------------------------------------------------------------------

def log(msg: str):
    print(msg, flush=True)


def run_cmd(cmd: List[str], timeout: int = 20) -> tuple[int, str, str, float]:
    """运行命令，返回 (returncode, stdout, stderr, elapsed_s)"""
    started = time.monotonic()
    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
            env=os.environ,
        )
        elapsed = time.monotonic() - started
        return proc.returncode, proc.stdout, proc.stderr, elapsed
    except subprocess.TimeoutExpired:
        elapsed = time.monotonic() - started
        return -1, "", f"timed out after {timeout}s", elapsed
    except Exception as e:
        elapsed = time.monotonic() - started
        return -1, "", str(e), elapsed


def parse_json_output(stdout: str, stderr: str) -> Optional[Dict[str, Any]]:
    """尝试从 stdout 解析 JSON，支持 bb-browser 输出的 banner 信息"""
    raw = stdout.strip()
    if not raw:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        pass
    try:
        start = raw.index("{")
        end = raw.rindex("}") + 1
        return json.loads(raw[start:end])
    except (ValueError, json.JSONDecodeError):
        return None


# ------------------------------------------------------------------
# 探测逻辑
# ------------------------------------------------------------------

def check_probe(cmd_info: Dict[str, Any]) -> Dict[str, Any]:
    """对单条探测命令执行检查，返回结果摘要"""
    name = cmd_info["name"]
    cmd = cmd_info["cmd"]
    timeout = cmd_info["timeout"]
    expect_key = cmd_info.get("expect_key", "data")

    rc, stdout, stderr, elapsed = run_cmd(cmd, timeout=timeout)
    result: Dict[str, Any] = {
        "name": name,
        "cmd": " ".join(cmd),
        "returncode": rc,
        "elapsed_s": round(elapsed, 2),
        "ok": False,
        "item_count": 0,
        "error": None,
        "sample": None,
    }

    if rc != 0:
        result["error"] = (stderr or stdout or "non-zero exit")[:150]
        return result

    data = parse_json_output(stdout, stderr)
    if data is None:
        result["error"] = "cannot parse JSON output"
        return result

    if isinstance(data, dict):
        items = data.get(expect_key)
        if not isinstance(items, list):
            items = data.get("data", {}).get(expect_key) if isinstance(data.get("data"), dict) else None
        if not isinstance(items, list):
            items = data.get("results") or data.get("items") or data.get("data", {}).get("items")
        if not isinstance(items, list):
            if isinstance(data.get("count"), int) and data["count"] > 0:
                items = [{}] * data["count"]
            elif isinstance(data.get("data", {}).get("count"), int) and data["data"]["count"] > 0:
                items = [{}] * data["data"]["count"]
    elif isinstance(data, list):
        items = data
    else:
        items = []

    count = len(items) if isinstance(items, list) else 0
    result["item_count"] = count
    result["ok"] = count > 0
    if count > 0 and isinstance(items[0], dict):
        sample = (
            items[0].get("title")
            or items[0].get("text")
            or items[0].get("content")
            or ""
        )
        result["sample"] = str(sample)[:80]

    return result


def probe_all() -> tuple[bool, Dict[str, Any]]:
    """执行所有探测命令，返回 (any_ok, all_results)"""
    all_results: Dict[str, Any] = {}
    any_ok = False

    for cmd_info in PROBE_COMMANDS:
        result = check_probe(cmd_info)
        all_results[result["name"]] = result
        if result["ok"]:
            any_ok = True

    return any_ok, all_results


# ------------------------------------------------------------------
# 重启逻辑（inlined from browser-restart/restart_browser.py）
# 不再跨技能调用，确保自包含
# ------------------------------------------------------------------

def read_cmdline(pid: int) -> str:
    try:
        return (
            Path(f"/proc/{pid}/cmdline")
            .read_bytes()
            .replace(b"\x00", b" ")
            .decode("utf-8", "ignore")
            .strip()
        )
    except Exception:
        return ""


def find_processes(predicate, exclude_pids=None):
    exclude = set(exclude_pids or [])
    found = []
    for name in os.listdir("/proc"):
        if not name.isdigit():
            continue
        pid = int(name)
        if pid in exclude:
            continue
        cmd = read_cmdline(pid)
        if cmd and predicate(cmd):
            found.append((pid, cmd))
    return found


def wait_dead(pids, timeout=8):
    end = time.time() + timeout
    while time.time() < end:
        alive = []
        for pid in pids:
            try:
                os.kill(pid, 0)
                alive.append(pid)
            except ProcessLookupError:
                pass
        if not alive:
            return True
        time.sleep(0.5)
    return False


def kill_processes(pairs, name: str):
    if not pairs:
        log(f"[INFO] No {name} processes found")
        return
    pids = [pid for pid, _ in pairs]
    log(f"[INFO] Stopping {name}: {pids}")
    for pid in pids:
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
    if wait_dead(pids, 8):
        return
    log(f"[WARN] {name} still alive after SIGTERM, sending SIGKILL")
    for pid in pids:
        try:
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    wait_dead(pids, 5)


def wait_for_cdp(port: int, timeout=30):
    end = time.time() + timeout
    while time.time() < end:
        try:
            req = urllib.request.Request(f"http://127.0.0.1:{port}/json/version")
            with urllib.request.urlopen(req, timeout=3) as resp:
                body = resp.read().decode("utf-8", "ignore")
                return json.loads(body)
        except Exception:
            time.sleep(1)
    return None


def do_restart(chrome_bin: str, display: str, cdp_port: int, wait_seconds: int) -> bool:
    """
    执行完整的浏览器重启流程（inlined，不调用外部脚本）。
    返回 True 表示重启完成，False 表示失败。
    """
    self_pid = os.getpid()
    parent_pid = os.getppid()
    exclude_pids = {self_pid, parent_pid}

    log("[INFO] Stopping bb-browser daemon and all Chrome processes...")

    # 杀掉所有 Chrome
    all_chrome_pairs = find_processes(
        lambda cl: (
            "/chrome " in f" {cl} "
            or "google-chrome" in cl
            or "chromium" in cl
        ),
        exclude_pids=exclude_pids,
    )
    kill_processes(all_chrome_pairs, "Chrome")

    # 杀掉 bb-browser daemon
    daemon_pairs = find_processes(
        lambda cl: ("node" in cl or "bb-browser" in cl) and "dist/cli.js" in cl,
        exclude_pids=exclude_pids,
    )
    kill_processes(daemon_pairs, "bb-browser daemon")
    time.sleep(2)

    # 启动 Chrome
    Path(FIXED_USER_DATA_DIR).mkdir(parents=True, exist_ok=True)
    chrome_cmd = [
        "env", f"DISPLAY={display}", "nohup", chrome_bin,
        f"--remote-debugging-port={cdp_port}",
        f"--user-data-dir={FIXED_USER_DATA_DIR}",
        "--no-first-run",
        "--no-default-browser-check",
        "--disable-sync",
        "--disable-background-networking",
        "--disable-component-update",
        "--disable-features=Translate,MediaRouter",
        "--disable-session-crashed-bubble",
        "--hide-crash-restore-bubble",
        DEFAULT_ZHIHU_URL,
    ]
    shell_parts = []
    for p in chrome_cmd:
        if " " in p:
            shell_parts.append(f'"{p}"')
        else:
            shell_parts.append(p)
    shell_cmd = " ".join(shell_parts)
    log(f"[INFO] Launching Chrome: {shell_cmd}")
    subprocess.Popen(
        f"nohup {shell_cmd} > {CHROME_LOG} 2>&1 &",
        shell=True,
        executable="/bin/bash",
        start_new_session=True,
    )

    # 等待 CDP 就绪
    cdp = wait_for_cdp(cdp_port, timeout=30)
    if not cdp:
        log(f"[FAIL] Chrome CDP did not become ready in time")
        return False
    log(f"[INFO] Chrome CDP ready: {cdp.get('Browser', 'unknown')}")

    # 写入 cdp-port 文件，daemon 启动时会自动重连
    MANAGED_BROWSER_DIR.mkdir(parents=True, exist_ok=True)
    MANAGED_PORT_FILE.write_text(str(cdp_port))
    log(f"[INFO] Written cdp-port={cdp_port} to {MANAGED_PORT_FILE}")

    # 等待知乎页面加载完成（cookie 写入）
    log(f"[INFO] Waiting {wait_seconds}s for zhihu page to load...")
    time.sleep(wait_seconds)

    log("[INFO] Restart complete.")
    return True


# ------------------------------------------------------------------
# 主入口
# ------------------------------------------------------------------

def main() -> int:
    print("🔍 bb-browser 可用性探测\n", flush=True)

    # 第一轮：轻量探测
    any_ok, all_results = probe_all()
    RESULT["probe_results"] = all_results
    RESULT["bb_browser_ok"] = any_ok

    ok_names = [name for name, r in all_results.items() if r["ok"]]
    fail_names = [name for name, r in all_results.items() if not r["ok"]]

    print(f"📊 探测结果（{len(all_results)} 条命令）：", flush=True)
    for name, r in all_results.items():
        icon = "✅" if r["ok"] else "❌"
        elapsed = f"{r['elapsed_s']}s"
        if r["ok"]:
            print(f"  {icon} {name}: {r['item_count']} 条 {elapsed}")
            if r.get("sample"):
                print(f"     示例: {r['sample']}")
        else:
            err = r.get("error") or "unknown error"
            print(f"  {icon} {name}: {err} ({elapsed})")

    if any_ok:
        RESULT["final_ok"] = True
        RESULT["message"] = f"bb-browser 可用，{len(ok_names)}/{len(all_results)} 探测成功"
        print(f"\n✅ {RESULT['message']}", flush=True)
        print(json.dumps(RESULT, ensure_ascii=False, indent=2))
        return 0

    # 第一轮全部失败 → 执行本地重启（不调用外部脚本，不再次探测）
    print(
        f"\n⚠️  bb-browser 全部探测失败（{len(fail_names)} 条），"
        "准备重启浏览器...",
        flush=True,
    )
    RESULT["restart_attempted"] = True

    restart_ok = do_restart(
        chrome_bin=DEFAULT_CHROME_BIN,
        display=DEFAULT_DISPLAY,
        cdp_port=FIXED_CDP_PORT,
        wait_seconds=8,
    )
    RESULT["restart_success"] = restart_ok

    if restart_ok:
        # 重启完成后直接返回成功，不再次探测
        # 后续各 fetch 步骤会自行验证 bb-browser 是否真的可用
        RESULT["final_ok"] = True
        RESULT["message"] = (
            "浏览器重启完成，各 bb-browser 源将由后续 fetch 步骤自行验证"
        )
        print(f"\n✅ {RESULT['message']}", flush=True)
    else:
        RESULT["final_ok"] = False
        RESULT["message"] = "浏览器重启失败"
        print(f"\n❌ {RESULT['message']}", flush=True)

    print(json.dumps(RESULT, ensure_ascii=False, indent=2))
    return 0 if restart_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
