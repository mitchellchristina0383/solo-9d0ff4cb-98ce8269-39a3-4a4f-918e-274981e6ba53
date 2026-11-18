#!/usr/bin/env python3
"""网关本地自测（不依赖 docker，使用 stdlib + 四个 uvicorn 子进程）。

覆盖关键场景：
  1.  轮询分发
  2.  状态 API
  3.  空闲节点排空 -> 立即 drained=true
  4.  排空后新请求不进入该节点
  5.  有在途慢请求时排空 -> 等在途结束才完成，且原响应可取回
  6.  全部不可接流 -> 503
  7.  上游 5xx -> 不虚报成功、节点降级、在途归零
  8.  探测成功不解除排空（交错核心）
  9.  恢复不绕过故障探测（交错核心）
  10. 仅 GET：POST 转发路径返回 405
  11. /docs 可打开
  12. 排空等待超时 -> 202，在途数准确
  13. 上游挂起 -> 网关请求超时 504，不虚报成功、在途归零
  14. 多并发在途请求：排空须等全部结束，在途计数全程准确
  15. 未知节点管理接口返回 404
  热替换（16-22）：
  16. 成功热替换：新 GET 走新址，状态含旧址与 offline_safe
  17. 探测失败 / 非法 URL / 同址 / 未知节点：原地址与接流状态保留
  18. 慢请求与替换交错：旧请求钉住旧址取原响应，未结清不可确认
  19. 同节点并发替换只一轮生效（200 + 409）
  20. 排空与替换交错：排空中新址不接流、503、旧址未结清 409
  21. 超时未确认 -> offline_safe=false，排空回执如实反映，confirm 后放行
  22. 旧址探测结论不覆盖新址（代际守卫 stale_probe_skips）
"""
from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request

PYLIBS = os.environ.get("PYLIBS", "/tmp/pylibs")
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PYTHON = sys.executable

GW_PORT = 19080
U1_PORT = 19081
U2_PORT = 19082
U3_PORT = 19083

passed = 0
failed = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global passed, failed
    if cond:
        passed += 1
        print(f"  [PASS] {name}")
    else:
        failed += 1
        print(f"  [FAIL] {name} {detail}")


def http(method: str, url: str, timeout: float = 20.0, data: bytes | None = None):
    """返回 (status, headers, body_text)。4xx/5xx 不抛异常。"""
    req = urllib.request.Request(url, method=method, data=data)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, dict(resp.headers), resp.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), e.read().decode()


def jpost(url: str, payload: dict, timeout: float = 20.0):
    """POST JSON，返回 (status, body_dict)。4xx/5xx 不抛异常。"""
    data = json.dumps(payload).encode()
    req = urllib.request.Request(
        url, data=data, method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode())


def replace(node: str, new_url: str, timeout: float = 20.0):
    return jpost(
        f"http://127.0.0.1:{GW_PORT}/admin/nodes/{node}/replace",
        {"new_url": new_url}, timeout=timeout,
    )


def jpost_url(url: str, timeout: float = 20.0):
    """对完整 URL 发空体 POST（confirm 等无 body 接口），返回 (status, body_dict)。"""
    req = urllib.request.Request(url, data=b"", method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode())


def jget(url: str, timeout: float = 20.0):
    status, _, body = http("GET", url, timeout)
    return status, json.loads(body)


def hget(headers: dict, name: str):
    """大小写不敏感的响应头读取（urllib 保留服务器发来的小写头名）。"""
    lowered = name.lower()
    for k, v in headers.items():
        if k.lower() == lowered:
            return v
    return None


def wait_ready(url: str, timeout: float = 25.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            status, _, _ = http("GET", url, timeout=2)
            if status == 200:
                return True
        except Exception:
            pass
        time.sleep(0.3)
    return False


def spawn(module_dir: str, port: int, extra_env: dict | None = None):
    # -I 隔离模式会忽略 PYTHONPATH 且不含 cwd，故用 runpy 包装器显式注入依赖与应用目录
    launcher = (
        "import sys,runpy,os;"
        f"sys.path[:0]=[{PYLIBS!r}, os.getcwd()];"
        "sys.argv=['uvicorn']+sys.argv[1:];"
        "runpy.run_module('uvicorn', run_name='__main__')"
    )
    env = dict(os.environ)
    env.pop("PYTHONPATH", None)
    env.update(extra_env or {})
    proc = subprocess.Popen(
        [PYTHON, "-I", "-c", launcher, "app:app",
         "--host", "127.0.0.1", "--port", str(port), "--log-level", "warning"],
        cwd=module_dir,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    return proc


def kill(proc: subprocess.Popen) -> None:
    if proc.poll() is None:
        try:
            proc.send_signal(signal.SIGTERM)
            proc.wait(timeout=8)
        except Exception:
            proc.kill()


def node_status(node: str) -> dict:
    status, body = jget(f"http://127.0.0.1:{GW_PORT}/admin/status")
    assert status == 200
    return next(n for n in body["nodes"] if n["name"] == node)


def set_mode(port: int, mode: str) -> None:
    status, _, body = http("POST", f"http://127.0.0.1:{port}/control/mode?mode={mode}")
    assert status == 200, body


def main() -> int:
    procs = []
    try:
        print("== 启动三个上游和网关 ==")
        u1 = spawn(os.path.join(ROOT, "upstream"), U1_PORT, {"NODE_ID": "node1"})
        u2 = spawn(os.path.join(ROOT, "upstream"), U2_PORT, {"NODE_ID": "node2"})
        u3 = spawn(os.path.join(ROOT, "upstream"), U3_PORT, {"NODE_ID": "node3"})
        procs = [u1, u2, u3]
        assert wait_ready(f"http://127.0.0.1:{U1_PORT}/")
        assert wait_ready(f"http://127.0.0.1:{U2_PORT}/")
        assert wait_ready(f"http://127.0.0.1:{U3_PORT}/")

        gw_env = {
            "UPSTREAMS": f"node1=http://127.0.0.1:{U1_PORT},node2=http://127.0.0.1:{U2_PORT}",
            "PROBE_INTERVAL": "1",
            "PROBE_TIMEOUT": "1",
            "REQUEST_TIMEOUT": "15",
            "DRAIN_DEFAULT_TIMEOUT": "10",
        }
        gw = spawn(os.path.join(ROOT, "gateway"), GW_PORT, gw_env)
        procs.append(gw)
        assert wait_ready(f"http://127.0.0.1:{GW_PORT}/docs"), "网关启动失败"
        time.sleep(1.2)  # 等首轮探测结果稳定

        # 1. 轮询分发
        print("== 1. 轮询分发 ==")
        seen = []
        for _ in range(4):
            status, body = jget(f"http://127.0.0.1:{GW_PORT}/proxy/echo")
            assert status == 200, body
            seen.append(body["node"])
        check("4 个请求在两节点间交替", seen == ["node1", "node2", "node1", "node2"], str(seen))
        st = node_status("node1")
        check("rr_hits 各 2 次", st["rr_hits"] == 2, str(st))
        check("在途计数已归零", st["inflight"] == 0, str(st))

        # 2. 状态 API 内容
        print("== 2. 状态 API ==")
        status, body = jget(f"http://127.0.0.1:{GW_PORT}/admin/status")
        check("两节点均健康", all(n["healthy"] for n in body["nodes"]))
        check("两节点均未排空", all(not n["draining"] for n in body["nodes"]))
        check("eligible 含两节点", sorted(body["eligible_nodes"]) == ["node1", "node2"])

        # 3. 空闲节点排空立即完成
        print("== 3. 空闲节点排空 ==")
        status, _, body = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node1/drain?timeout=5")
        b = json.loads(body)
        check("排空返回 200", status == 200, body)
        check("drained=true", b.get("drained") is True, body)
        check("inflight=0", b.get("inflight") == 0, body)
        st = node_status("node1")
        check("状态显示 draining", st["draining"] is True and st["eligible"] is False)

        # 4. 排空后新请求不进入 node1
        print("== 4. 排空后新请求只去 node2 ==")
        nodes = set()
        for _ in range(3):
            status, body = jget(f"http://127.0.0.1:{GW_PORT}/proxy/echo")
            nodes.add(body["node"])
        check("新请求全部落在 node2", nodes == {"node2"}, str(nodes))

        # 5. 在途慢请求 + 排空交错：排空等在途结束，原响应可取回
        print("== 5. 在途慢请求期间排空 node2 ==")
        result = {}

        def slow_call():
            result["status"], _, result["body"] = http(
                "GET", f"http://127.0.0.1:{GW_PORT}/proxy/slow?seconds=3", timeout=20
            )

        t = threading.Thread(target=slow_call)
        t.start()
        # 等到请求确实进入 node2
        inflight_seen = 0
        deadline = time.time() + 5
        while time.time() < deadline:
            st2 = node_status("node2")
            inflight_seen = max(inflight_seen, st2["inflight"])
            if st2["inflight"] == 1:
                break
            time.sleep(0.1)
        check("慢请求已进入 node2 且 inflight=1", inflight_seen == 1, str(st2))

        drain_started = time.time()
        status, _, body = http(
            "POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node2/drain?timeout=10"
        )
        drain_elapsed = time.time() - drain_started
        b = json.loads(body)
        t.join(timeout=20)
        check("排空请求被在途请求阻塞约 3s", drain_elapsed >= 2.5, f"{drain_elapsed:.2f}s")
        check("排空最终成功 drained=true", status == 200 and b.get("drained") is True, body)
        check("慢请求原响应成功取回 200", result.get("status") == 200, str(result))
        check("响应来自 node2 且内容完整",
              json.loads(result["body"]).get("node") == "node2"
              and json.loads(result["body"]).get("slept", 0) >= 2.5,
              result.get("body"))
        check("结束后两节点在途均为 0",
              all(node_status(n)["inflight"] == 0 for n in ("node1", "node2")))

        # 6. 两节点都排空 -> 503
        print("== 6. 无可用节点返回 503 ==")
        status, _, body = http("GET", f"http://127.0.0.1:{GW_PORT}/proxy/echo")
        check("返回 503", status == 503, body)
        check("503 原因可解释", json.loads(body).get("error") == "no_eligible_upstream", body)

        # 7. 上游 5xx：不虚报成功 + 节点降级 + 在途归零
        print("== 7. 上游故障 (5xx) ==")
        # 先恢复 node1 以便发请求（node2 仍排空）
        status, _, _ = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node1/resume")
        assert status == 200
        set_mode(U1_PORT, "error")
        time.sleep(1.3)  # 等周期性探测先发现故障
        st = node_status("node1")
        check("探测已将 node1 标记为不健康", st["healthy"] is False, str(st))
        status, _, body = http("GET", f"http://127.0.0.1:{GW_PORT}/proxy/flaky")
        check("无健康节点时返回 503 而非虚假 200", status == 503, body)
        # 临时恢复健康标志位才能真正打到 5xx（模拟请求路径上的失败降级）
        set_mode(U1_PORT, "healthy")
        time.sleep(1.3)
        st = node_status("node1")
        check("探测恢复后 node1 健康", st["healthy"] is True, str(st))
        set_mode(U1_PORT, "error")  # 不等待探测，让请求先撞上 5xx
        status, _, body = http("GET", f"http://127.0.0.1:{GW_PORT}/proxy/flaky")
        check("5xx 状态如实返回", status == 500, f"status={status} body={body}")
        time.sleep(0.3)
        st = node_status("node1")
        check("请求失败后节点即时降级", st["healthy"] is False, str(st))
        check("失败后在途计数仍归零", st["inflight"] == 0, str(st))

        # 8. 交错核心：探测成功不得解除排空
        print("== 8. 探测成功不解除排空 ==")
        set_mode(U1_PORT, "healthy")
        # node1 当前未排空，先排空，再让周期性探测持续成功
        status, _, _ = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node1/drain?timeout=3")
        assert status == 200
        time.sleep(2.5)  # 跨越至少 2 轮探测，上游为 healthy，探测必然成功
        st = node_status("node1")
        check("多轮探测成功后排空标志仍在", st["draining"] is True, str(st))
        check("探测成功只更新健康位", st["healthy"] is True and st["eligible"] is False, str(st))

        # 9. 交错核心：恢复不绕过故障探测
        print("== 9. 恢复不绕过故障探测 ==")
        set_mode(U1_PORT, "error")
        time.sleep(1.3)  # 探测确认故障
        st_before = node_status("node1")
        check("恢复前 node1 不健康", st_before["healthy"] is False, str(st_before))
        status, _, body = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node1/resume")
        b = json.loads(body)
        check("resume 清除排空但节点仍不可接流",
              status == 200 and b["draining"] is False and b["eligible"] is False, body)
        st = node_status("node1")
        check("draining=False 且 healthy=False",
              st["draining"] is False and st["healthy"] is False, str(st))
        # 故障节点上请求不会被分配：node2 仍排空 -> 503
        status, _, _ = http("GET", f"http://127.0.0.1:{GW_PORT}/proxy/echo")
        check("故障节点不会被轮询选中 (503)", status == 503)
        # 上游恢复后经探测自动回到可接流
        set_mode(U1_PORT, "healthy")
        time.sleep(1.5)
        st = node_status("node1")
        check("探测恢复后节点重新 eligible", st["eligible"] is True, str(st))
        status, body = jget(f"http://127.0.0.1:{GW_PORT}/proxy/echo")
        check("流量恢复进入 node1", status == 200 and body["node"] == "node1")

        # 10. 仅 GET 转发
        print("== 10. 非 GET 方法被拒 ==")
        status, _, body = http("POST", f"http://127.0.0.1:{GW_PORT}/proxy/echo", data=b"x")
        check("POST 转发路径返回 405", status == 405, f"status={status}")

        # 11. /docs 可打开
        print("== 11. /docs 与 openapi.json ==")
        status, _, body = http("GET", f"http://127.0.0.1:{GW_PORT}/docs")
        check("/docs 返回 200", status == 200 and "swagger" in body.lower())
        status, body = jget(f"http://127.0.0.1:{GW_PORT}/openapi.json")
        paths = body["paths"]
        check("openapi 含管理、热替换与转发接口",
              "/admin/nodes/{name}/drain" in paths
              and "/admin/nodes/{name}/replace" in paths
              and "/proxy/{path}" in paths)

        # 12. 排空等待超时：202 + 在途准确
        print("== 12. 排空超时返回 202 ==")
        result12 = {}

        def slow_call12():
            result12["status"], _, result12["body"] = http(
                "GET", f"http://127.0.0.1:{GW_PORT}/proxy/slow?seconds=4", timeout=15
            )

        t12 = threading.Thread(target=slow_call12)
        t12.start()
        deadline = time.time() + 5
        while time.time() < deadline and node_status("node1")["inflight"] != 1:
            time.sleep(0.1)
        status, _, body = http(
            "POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node1/drain?timeout=1"
        )
        b = json.loads(body)
        check("短超时排空返回 202", status == 202, body)
        check("drained=false 且 inflight=1", b.get("drained") is False and b.get("inflight") == 1, body)
        t12.join(timeout=15)
        check("在途慢请求仍拿到完整 200", result12.get("status") == 200, str(result12))
        time.sleep(0.3)
        # 在途归零后再次排空（节点已在排空中）应立即完成
        status, _, body = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node1/drain?timeout=3")
        b = json.loads(body)
        check("在途归零后排空立即完成", status == 200 and b["drained"] is True, body)

        # 13. 请求超时：重启网关为短超时，上游 hang（探测与业务请求都会挂住）
        print("== 13. 上游挂起 -> 网关 504 超时 ==")
        kill(gw)
        procs.remove(gw)
        short_env = dict(gw_env)
        short_env["REQUEST_TIMEOUT"] = "1"
        gw = spawn(os.path.join(ROOT, "gateway"), GW_PORT, short_env)
        procs.append(gw)
        assert wait_ready(f"http://127.0.0.1:{GW_PORT}/docs")
        set_mode(U1_PORT, "healthy")
        # 等首轮探测把 node1 标记为健康可接流（网关重启后状态全清）
        deadline = time.time() + 8
        while time.time() < deadline and not node_status("node1")["eligible"]:
            time.sleep(0.2)
        check("重启后 node1 经探测恢复可接流", node_status("node1")["eligible"] is True)
        # 切 hang 后立即发请求：赶在下一轮周期探测摘掉它之前验证网关自身的请求超时
        set_mode(U1_PORT, "hang")
        t0 = time.time()
        status, _, body = http("GET", f"http://127.0.0.1:{GW_PORT}/proxy/flaky", timeout=8)
        elapsed = time.time() - t0
        if status == 503:
            # 探测抢先把节点摘掉，则手动恢复后重试一次，确保覆盖请求超时路径
            set_mode(U1_PORT, "healthy")
            deadline = time.time() + 6
            while time.time() < deadline and not node_status("node1")["eligible"]:
                time.sleep(0.2)
            set_mode(U1_PORT, "hang")
            t0 = time.time()
            status, _, body = http("GET", f"http://127.0.0.1:{GW_PORT}/proxy/flaky", timeout=8)
            elapsed = time.time() - t0
        check("挂起请求在约 1s 内返回 504", status == 504 and elapsed < 4,
              f"status={status} elapsed={elapsed:.1f}s body={body}")
        check("未虚报成功", status != 200)
        time.sleep(0.3)
        st = node_status("node1")
        check("超时后在途计数准确归零", st["inflight"] == 0, str(st))
        check("超时后节点被标记不健康", st["healthy"] is False, str(st))
        set_mode(U1_PORT, "healthy")

        # 14. 多个并发在途请求：排空须等全部结束，在途计数全程准确
        print("== 14. 多并发在途请求排空 ==")
        # 恢复正常请求超时并重启网关（清空场景 13 的短超时与节点状态）
        kill(gw)
        procs.remove(gw)
        gw = spawn(os.path.join(ROOT, "gateway"), GW_PORT, gw_env)
        procs.append(gw)
        assert wait_ready(f"http://127.0.0.1:{GW_PORT}/docs")
        deadline = time.time() + 8
        while time.time() < deadline and not node_status("node1")["eligible"]:
            time.sleep(0.2)
        check("重启后 node1 可接流", node_status("node1")["eligible"] is True)
        # 先排空 node2，把流量集中到 node1
        status, _, _ = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node2/drain?timeout=5")
        assert status == 200
        results14 = []

        def slow_call14(idx: int):
            s, _, b = http("GET", f"http://127.0.0.1:{GW_PORT}/proxy/slow?seconds=3", timeout=20)
            results14.append((idx, s, b))

        threads = [threading.Thread(target=slow_call14, args=(i,)) for i in range(4)]
        for th in threads:
            th.start()
        deadline = time.time() + 5
        while time.time() < deadline and node_status("node1")["inflight"] < 4:
            time.sleep(0.1)
        st = node_status("node1")
        check("4 个在途请求计数准确", st["inflight"] == 4, str(st))
        t0 = time.time()
        status, _, body = http(
            "POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node1/drain?timeout=15"
        )
        elapsed = time.time() - t0
        b = json.loads(body)
        for th in threads:
            th.join(timeout=20)
        check("排空阻塞到全部 4 个在途请求结束", elapsed >= 2.5 and b.get("drained") is True,
              f"elapsed={elapsed:.1f}s body={body}")
        check("4 个慢请求全部取回完整 200",
              len(results14) == 4 and all(s == 200 for _, s, _ in results14),
              str(results14))
        check("全部结束后在途归零", node_status("node1")["inflight"] == 0)

        # 15. 未知节点管理接口返回 404
        print("== 15. 未知节点 404 ==")
        status, _, body = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/ghost/drain")
        check("排空未知节点返回 404", status == 404, body)
        status, _, body = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/ghost/resume")
        check("恢复未知节点返回 404", status == 404, body)

        # ---- 热替换系列：先重启网关，回到干净状态 ----
        kill(gw)
        procs.remove(gw)
        gw = spawn(os.path.join(ROOT, "gateway"), GW_PORT, gw_env)
        procs.append(gw)
        assert wait_ready(f"http://127.0.0.1:{GW_PORT}/docs")
        time.sleep(1.5)
        assert node_status("node1")["eligible"] and node_status("node2")["eligible"]
        u1_url = f"http://127.0.0.1:{U1_PORT}"
        u2_url = f"http://127.0.0.1:{U2_PORT}"
        u3_url = f"http://127.0.0.1:{U3_PORT}"

        # 16. 成功热替换：新 GET 走新址，旧址记账可见
        print("== 16. 成功热替换 ==")
        # 先排空 node2，让验证流量全部落在 node1
        status, _, _ = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node2/drain?timeout=5")
        assert status == 200
        status, body = jget(f"http://127.0.0.1:{GW_PORT}/proxy/echo")
        assert status == 200 and body["node"] == "node1"
        status, rb = replace("node1", u3_url)
        check("替换探测成功返回 200", status == 200 and rb.get("replaced") is True, str(rb))
        check("回执含新旧地址与代际",
              rb.get("previous_url") == u1_url and rb.get("base_url") == u3_url
              and rb.get("generation") == 1 and rb.get("offline_safe") is True, str(rb))
        status, body = jget(f"http://127.0.0.1:{GW_PORT}/proxy/echo")
        check("新请求转到新址 node3", status == 200 and body["node"] == "node3", body)
        status, headers, _ = http("GET", f"http://127.0.0.1:{GW_PORT}/proxy/echo")
        check("响应头标注实际转发 URL", hget(headers, "X-Upstream-Url") == u3_url, str(headers))
        st = node_status("node1")
        check("状态 base_url 已更新为新址", st["base_url"] == u3_url, str(st))
        check("状态含旧址块且可下线",
              st["old_address"] is not None and st["old_address"]["url"] == u1_url
              and st["old_address"]["offline_safe"] is True
              and st["previous"]["inflight"] == 0 and st["offline_safe"] is True, str(st))
        check("新址健康且节点未排空", st["healthy"] is True and st["draining"] is False, str(st))
        # 结清并确认旧址，然后切回 u1，恢复场景
        status, cb = jpost_url(f"http://127.0.0.1:{GW_PORT}/admin/nodes/node1/replace/confirm")
        check("确认旧址返回 200", status == 200 and cb.get("confirmed") is True, str(cb))

        # 17. 探测失败 / 非法输入：原地址与接流状态原样保留
        print("== 17. 探测失败与非法输入 ==")
        before = node_status("node1")
        status, rb = replace("node1", "http://127.0.0.1:1/nope")  # 无监听 -> 连接失败
        check("探测失败返回 502", status == 502 and rb.get("replaced") is False
              and rb.get("probe_ok") is False, str(rb))
        after = node_status("node1")
        check("失败后原地址保留", after["base_url"] == u3_url == before["base_url"], str(after))
        check("失败后接流状态保留", after["eligible"] == before["eligible"] is True, str(after))
        status, body = jget(f"http://127.0.0.1:{GW_PORT}/proxy/echo")
        check("失败后流量仍走新址 node3", status == 200 and body["node"] == "node3", body)
        status, rb = replace("node1", "ftp://bad-scheme")
        check("非法 URL 返回 400", status == 400 and rb.get("error") == "invalid_url", str(rb))
        status, rb = replace("node1", u3_url)
        check("与当前地址相同返回 400", status == 400 and rb.get("error") == "same_as_current", str(rb))
        status, rb = replace("ghost", u1_url)
        check("替换未知节点返回 404", status == 404, str(rb))
        st = node_status("node1")
        check("各类拒绝均未改动状态", st["base_url"] == u3_url and st["generation"] == 1, str(st))

        # 18. 慢请求与替换交错：旧请求钉住旧址，未结清不可确认
        print("== 18. 慢请求与热替换交错 ==")
        result18 = {}

        def slow_call18():
            result18["status"], result18["headers"], result18["body"] = http(
                "GET", f"http://127.0.0.1:{GW_PORT}/proxy/slow?seconds=3", timeout=20
            )

        t18 = threading.Thread(target=slow_call18)
        t18.start()
        deadline = time.time() + 5
        while time.time() < deadline and node_status("node1")["inflight"] != 1:
            time.sleep(0.1)
        check("慢请求已进入当前地址 (node3)", node_status("node1")["inflight"] == 1)
        t0 = time.time()
        status, rb = replace("node1", u1_url)
        swap_elapsed = time.time() - t0
        check("替换不等待在途、立即生效", status == 200 and swap_elapsed < 1.0
              and rb.get("base_url") == u1_url, f"{swap_elapsed:.2f}s {rb}")
        st = node_status("node1")
        check("旧址 node3 在途=1，节点不可下线",
              st["previous"]["url"] == u3_url and st["previous"]["inflight"] == 1
              and st["offline_safe"] is False, str(st))
        status, _, nbody = http("GET", f"http://127.0.0.1:{GW_PORT}/proxy/echo")
        check("替换后新请求走新址 node1", status == 200
              and json.loads(nbody).get("node") == "node1", nbody)
        status, cb = jpost_url(f"http://127.0.0.1:{GW_PORT}/admin/nodes/node1/replace/confirm")
        check("旧址有在途时确认被拒 409", status == 409
              and cb.get("error") == "previous_still_inflight", str(cb))
        # 旧址未结清时发起再一轮替换 -> 409，不覆盖 previous
        status, rb = replace("node1", u2_url)
        check("旧址未结清时新一轮替换 409", status == 409
              and rb.get("error") == "previous_not_settled", str(rb))
        t18.join(timeout=20)
        check("在途慢请求未被截断，取回旧址完整 200",
              result18.get("status") == 200
              and json.loads(result18["body"]).get("node") == "node3"
              and hget(result18["headers"], "X-Upstream-Url") == u3_url, str(result18))
        time.sleep(0.3)
        st = node_status("node1")
        check("慢请求结束后旧址在途归零、可下线",
              st["previous"]["inflight"] == 0 and st["offline_safe"] is True, str(st))
        status, cb = jpost_url(f"http://127.0.0.1:{GW_PORT}/admin/nodes/node1/replace/confirm")
        check("结清后确认成功", status == 200 and cb.get("offline_safe") is True, str(cb))

        # 19. 同一节点并发替换只允许一轮生效
        print("== 19. 并发替换串行化 ==")
        outcomes = {}
        # 用屏障让两个请求几乎同时进入网关，提高锁竞争命中率
        barrier = threading.Barrier(2)

        def do_replace(tag: str, url: str):
            barrier.wait()
            outcomes[tag] = replace("node1", url)

        th_a = threading.Thread(target=do_replace, args=("a", u3_url))
        th_b = threading.Thread(target=do_replace, args=("b", u2_url))
        th_a.start()
        th_b.start()
        th_a.join(timeout=20)
        th_b.join(timeout=20)
        codes = sorted(s for s, _ in outcomes.values())
        check("并发替换结果为 200 + 409", codes == [200, 409], str(outcomes))
        loser = next(b for b in outcomes.values() if b[0] == 409)
        check("失败方提示 replace_in_progress",
              loser[1].get("error") == "replace_in_progress", str(loser))
        winner_url = outcomes["a"][1].get("base_url") if outcomes["a"][0] == 200 \
            else outcomes["b"][1].get("base_url")
        st = node_status("node1")
        check("只有一轮替换生效，状态与胜方一致", st["base_url"] == winner_url
              and st["generation"] == 3, str(st))
        # 把节点切回 u1 并确认旧址，供后续场景使用
        if st["base_url"] != u1_url:
            status, _ = jpost_url(f"http://127.0.0.1:{GW_PORT}/admin/nodes/node1/replace/confirm")
            assert status == 200
            status, rb = replace("node1", u1_url)
            assert status == 200, str(rb)
        status, _ = jpost_url(f"http://127.0.0.1:{GW_PORT}/admin/nodes/node1/replace/confirm")
        assert status == 200

        # 20. 排空与替换交错：排空中新址不接流、503、回执含旧址
        print("== 20. 排空与热替换交错 ==")
        status, _, _ = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node2/resume")
        assert status == 200
        time.sleep(1.5)
        status, rb = replace("node2", u3_url)
        check("排空场景准备：node2 切到新址", status == 200 and rb["base_url"] == u3_url, str(rb))
        status, _, _ = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node2/drain?timeout=5")
        assert status == 200
        st = node_status("node2")
        check("排空后新址同样不接流", st["draining"] is True and st["eligible"] is False, str(st))
        # node1 仍可接流；再排空 node1 -> 全局 503
        status, _, _ = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node1/drain?timeout=5")
        assert status == 200
        status, _, body = http("GET", f"http://127.0.0.1:{GW_PORT}/proxy/echo")
        check("全排空中（含替换后的新址）返回 503", status == 503, body)
        # 排空中仍允许替换操作本身，但替换后节点依旧不接流
        status, rb = replace("node2", u2_url)
        check("排空中替换地址成功但节点仍不接流",
              status == 200 and rb["base_url"] == u2_url and rb["eligible"] is False, str(rb))
        status, _, body = http("GET", f"http://127.0.0.1:{GW_PORT}/proxy/echo")
        check("替换不解除排空，仍 503", status == 503, body)
        status, _, body = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node2/drain?timeout=5")
        db = json.loads(body)
        check("排空回执包含旧址块与 offline_safe",
              status == 200 and db["drained"] is True and db["offline_safe"] is True
              and db["old_address"] is not None and db["old_address"]["url"] == u3_url, body)
        # 恢复两节点
        status, _, _ = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node1/resume")
        assert status == 200
        status, _, _ = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node2/resume")
        assert status == 200
        time.sleep(1.5)

        # 21. 超时未确认 -> offline_safe=false，排空回执如实，confirm 后放行
        print("== 21. 旧址超时未确认与可下线判定 ==")
        kill(gw)
        procs.remove(gw)
        short_env = dict(gw_env)
        short_env["REQUEST_TIMEOUT"] = "1"
        gw = spawn(os.path.join(ROOT, "gateway"), GW_PORT, short_env)
        procs.append(gw)
        assert wait_ready(f"http://127.0.0.1:{GW_PORT}/docs")
        deadline = time.time() + 8
        while time.time() < deadline and not node_status("node1")["eligible"]:
            time.sleep(0.2)
        assert node_status("node1")["eligible"]
        set_mode(U1_PORT, "hang")
        # 抢在周期探测前打一个必超时的请求（探测先摘除则重试，与场景 13 同策略）
        status, _, _ = http("GET", f"http://127.0.0.1:{GW_PORT}/proxy/flaky", timeout=8)
        if status != 504:
            set_mode(U1_PORT, "healthy")
            deadline = time.time() + 6
            while time.time() < deadline and not node_status("node1")["eligible"]:
                time.sleep(0.2)
            set_mode(U1_PORT, "hang")
            status, _, _ = http("GET", f"http://127.0.0.1:{GW_PORT}/proxy/flaky", timeout=8)
        check("现地址请求返回 504", status == 504, f"status={status}")
        set_mode(U1_PORT, "healthy")
        # 把节点替换到健康的 u3：u1 成为带未确认超时的旧址
        deadline = time.time() + 6
        while time.time() < deadline:
            status, rb = replace("node1", u3_url)
            if status == 200:
                break
            time.sleep(0.5)
        check("恢复后可替换到 u3", status == 200, str(rb))
        st = node_status("node1")
        check("旧址存在未确认超时，节点不可下线",
              st["previous"]["unconfirmed_timeouts"] >= 1
              and st["previous"]["offline_safe"] is False
              and st["offline_safe"] is False, str(st))
        check("新址健康可接流，未被旧址结论污染",
              st["healthy"] is True and st["eligible"] is True, str(st))
        status, _, body = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node1/drain?timeout=3")
        db = json.loads(body)
        check("排空回执在未确认超时存在时不报告可下线",
              status == 200 and db["drained"] is True and db["offline_safe"] is False
              and db["old_address"]["unconfirmed_timeouts"] >= 1, body)
        status, cb = jpost_url(f"http://127.0.0.1:{GW_PORT}/admin/nodes/node1/replace/confirm")
        check("确认后清零并可下线", status == 200 and cb["offline_safe"] is True
              and cb["old_address"]["unconfirmed_timeouts"] == 0, str(cb))
        status, rb = replace("node1", u2_url)
        check("确认放行后新一轮替换成功", status == 200 and rb["base_url"] == u2_url, str(rb))

        # 22. 旧址探测结论不得覆盖新址（代际守卫）
        print("== 22. 代际守卫：旧探测结论丢弃 ==")
        kill(gw)
        procs.remove(gw)
        # 长探测超时 + 短间隔：保证挂在旧址上的探测必然跨越替换时刻
        guard_env = dict(gw_env)
        guard_env["PROBE_TIMEOUT"] = "4"
        guard_env["PROBE_INTERVAL"] = "1"
        gw = spawn(os.path.join(ROOT, "gateway"), GW_PORT, guard_env)
        procs.append(gw)
        assert wait_ready(f"http://127.0.0.1:{GW_PORT}/docs")
        time.sleep(1.5)
        assert node_status("node1")["eligible"]
        # u1 置 hang：周期探测会挂在旧址上（4s 超时）
        set_mode(U1_PORT, "hang")
        time.sleep(2.0)  # 至少一轮周期探测已发出并挂在 u1 上
        status, rb = replace("node1", u3_url)  # 替换探测打 u3（健康，立即返回）
        check("hang 的旧址不影响新址替换", status == 200 and rb["base_url"] == u3_url, str(rb))
        st = node_status("node1")
        check("替换后新址立即健康可接流", st["healthy"] is True and st["eligible"] is True, str(st))
        time.sleep(5.0)  # 等挂在旧址上、跨越替换时刻的探测超时返回
        st = node_status("node1")
        check("过期探测被代际守卫丢弃，新址保持健康",
              st["current"]["healthy"] is True and st["eligible"] is True
              and st["stale_probe_skips"] >= 1, str(st))
        check("旧址状态冻结（未被后续探测改写）",
              st["previous"]["url"] == u1_url and st["previous"]["healthy"] is True, str(st))
        status, body = jget(f"http://127.0.0.1:{GW_PORT}/proxy/echo")
        check("流量稳定走新址 node3", status == 200 and body["node"] == "node3", body)
        set_mode(U1_PORT, "healthy")

        print(f"\n==== 结果: {passed} passed, {failed} failed ====")
        return 1 if failed else 0

    finally:
        for p in procs:
            kill(p)


if __name__ == "__main__":
    sys.exit(main())
