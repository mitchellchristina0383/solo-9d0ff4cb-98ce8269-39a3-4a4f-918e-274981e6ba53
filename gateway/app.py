"""HTTP 入口网关（FastAPI + httpx）。

职责：
  * 仅转发 GET 请求到上游，按轮询（round-robin）选择「健康且未排空」的节点；
  * 周期性主动探测上游，探测结论只影响健康标志，绝不改动人工排空标志；
  * 提供排空 / 恢复 / 状态管理 API；所有节点都不可接流时返回 503；
  * 排空期间新请求不再进入该节点，已在途的请求继续等待原响应返回；
  * 排空 API 阻塞到在途请求全部结束后才报告 drained=true；
  * 超时 / 连接错误 / 5xx 不虚报成功，并同步刷新健康状态，在途计数靠 try/finally 保证准确；
  * 单节点地址热替换：先探测新址，失败则原样保留；成功后新 GET 走新址、旧请求钉住旧址
    取回原响应；旧址在途 / 未确认超时未结清前不得报告可下线，并阻止下一轮替换。
"""
from __future__ import annotations

import asyncio
import os
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Optional
from urllib.parse import urlparse

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel, Field

# ----------------------------- 配置 -----------------------------

UPSTREAMS_RAW = os.environ.get(
    "UPSTREAMS",
    "node1=http://upstream1:8000,node2=http://upstream2:8000",
)
PROBE_INTERVAL = float(os.environ.get("PROBE_INTERVAL", "2.0"))
PROBE_TIMEOUT = float(os.environ.get("PROBE_TIMEOUT", "1.5"))
REQUEST_TIMEOUT = float(os.environ.get("REQUEST_TIMEOUT", "15.0"))
DRAIN_DEFAULT_TIMEOUT = float(os.environ.get("DRAIN_DEFAULT_TIMEOUT", "30.0"))

# 透传响应时需要剥离的逐跳（hop-by-hop）头
_HOP_BY_HOP = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
    "content-length",
    "content-encoding",
}


def _parse_upstreams(raw: str) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        name, sep, url = item.partition("=")
        if not sep:
            raise ValueError(f"非法 UPSTREAMS 条目: {item!r}，应为 name=url")
        out.append((name.strip(), _normalize_url(url)))
    if not out:
        raise ValueError("至少需要配置一个上游节点")
    return out


def _normalize_url(raw: str) -> str:
    """校验并规范化 URL：仅允许 http/https，去掉末尾斜杠。"""
    parsed = urlparse(raw.strip())
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        raise ValueError(f"非法 URL: {raw!r}，需为 http(s)://host[:port] 形式")
    return raw.strip().rstrip("/")


# ----------------------------- 端点 / 节点状态 -----------------------------

@dataclass
class EndpointInfo:
    """一个上游地址（endpoint）的运行状态。

    热替换后旧址 endpoint 仍被在途请求引用，因此地址级状态独立成对象：
    请求在选中时钉住具体 endpoint，后续替换不影响已在途请求使用的 URL。
    """

    url: str
    healthy: bool = False
    # 该地址上的在途请求数（含已被替换为旧址、仍未结束的请求）
    inflight: int = 0
    # 请求超时后执行结果未人工确认的次数（连接错误 / 5xx 有明确响应，不计入）
    unconfirmed_timeouts: int = 0
    last_probe_at: Optional[float] = None
    last_probe_ok: Optional[bool] = None
    last_probe_detail: str = "尚未探测"
    # 作为旧址退役 / 被人工确认的时间戳
    retired_at: Optional[float] = None
    confirmed_at: Optional[float] = None

    def offline_safe(self) -> bool:
        """旧址是否可下线：无在途且无未确认超时。"""
        return self.inflight == 0 and self.unconfirmed_timeouts == 0

    def snapshot(self) -> dict:
        return {
            "url": self.url,
            "healthy": self.healthy,
            "inflight": self.inflight,
            "unconfirmed_timeouts": self.unconfirmed_timeouts,
            "offline_safe": self.offline_safe(),
            "last_probe_at": self.last_probe_at,
            "last_probe_ok": self.last_probe_ok,
            "last_probe_detail": self.last_probe_detail,
            "retired_at": self.retired_at,
            "confirmed_at": self.confirmed_at,
        }


@dataclass
class NodeState:
    name: str
    current: EndpointInfo
    # 上一任地址：保留至在途与未确认超时全部结清并被人工确认
    previous: Optional[EndpointInfo] = None
    # 地址代际：每次成功替换 +1，探测凭代际戳判断结论是否过期
    generation: int = 0
    # 排空标志：只由人工 drain/resume 写入，探测与替换永不触碰
    draining: bool = False
    rr_hits: int = 0
    success_count: int = 0
    failure_count: int = 0
    last_change_at: float = field(default_factory=time.time)
    # 因代际过期被丢弃的探测结论数（可观测，验证旧结论不覆盖新状态）
    stale_probe_skips: int = 0

    @property
    def base_url(self) -> str:
        return self.current.url

    @property
    def inflight(self) -> int:
        total = self.current.inflight
        if self.previous is not None:
            total += self.previous.inflight
        return total

    def can_serve(self) -> bool:
        # 排空期间现地址（含替换后的新址）一律不接流
        return self.current.healthy and not self.draining

    def offline_safe(self) -> bool:
        """节点（旧址）是否可下线：无旧址，或旧址已结清。"""
        return self.previous is None or self.previous.offline_safe()

    def snapshot(self) -> dict:
        snap = {
            "name": self.name,
            # 兼容字段：base_url / healthy 始终反映现地址
            "base_url": self.current.url,
            "healthy": self.current.healthy,
            "draining": self.draining,
            "eligible": self.can_serve(),
            "inflight": self.inflight,
            "rr_hits": self.rr_hits,
            "success_count": self.success_count,
            "failure_count": self.failure_count,
            "last_change_at": self.last_change_at,
            "generation": self.generation,
            "stale_probe_skips": self.stale_probe_skips,
            "offline_safe": self.offline_safe(),
            "current": self.current.snapshot(),
            "previous": self.previous.snapshot() if self.previous else None,
            # 便于值班员直接阅读的旧址块（与 previous 同源）
            "old_address": (
                {
                    "url": self.previous.url,
                    "inflight": self.previous.inflight,
                    "unconfirmed_timeouts": self.previous.unconfirmed_timeouts,
                    "offline_safe": self.previous.offline_safe(),
                    "confirmed_at": self.previous.confirmed_at,
                }
                if self.previous
                else None
            ),
            "last_probe_at": self.current.last_probe_at,
            "last_probe_ok": self.current.last_probe_ok,
            "last_probe_detail": self.current.last_probe_detail,
        }
        return snap


class ReplaceRequest(BaseModel):
    new_url: str = Field(..., min_length=1, description="新实例地址，http(s)://host[:port]")


class Gateway:
    def __init__(self, nodes: list[NodeState]) -> None:
        self.nodes = nodes
        self._by_name = {n.name: n for n in nodes}
        # 一把条件变量保护所有节点状态；请求结束 / 排空等待都在其上通知
        self.cond = asyncio.Condition()
        self._rr = 0
        self.client: Optional[httpx.AsyncClient] = None
        self._probe_task: Optional[asyncio.Task] = None
        # 每节点一把替换锁：同一节点并发替换只允许一轮生效
        self._replace_locks = {n.name: asyncio.Lock() for n in nodes}

    def get(self, name: str) -> Optional[NodeState]:
        return self._by_name.get(name)

    async def pick_node(self) -> tuple[Optional[NodeState], Optional[EndpointInfo]]:
        """轮询选取健康且未排空的节点，并立即登记在途计数。

        返回 (节点, 被钉住的 endpoint)；调用方在整个请求周期内只使用该 endpoint，
        即使期间发生热替换，在途请求也仍访问原地址。
        """
        async with self.cond:
            n = len(self.nodes)
            for offset in range(n):
                idx = (self._rr + offset) % n
                node = self.nodes[idx]
                if node.can_serve():
                    self._rr = (idx + 1) % n
                    endpoint = node.current
                    endpoint.inflight += 1
                    node.rr_hits += 1
                    return node, endpoint
            return None, None

    async def release_node(
        self, node: NodeState, endpoint: EndpointInfo, ok: bool
    ) -> None:
        async with self.cond:
            endpoint.inflight -= 1
            if ok:
                node.success_count += 1
            else:
                node.failure_count += 1
            # 在途归零可能让 drain 等待者满足条件
            self.cond.notify_all()

    async def mark_endpoint_failure(
        self,
        node: NodeState,
        endpoint: EndpointInfo,
        detail: str,
        unconfirmed: bool = False,
    ) -> None:
        """请求失败时的即时记账。

        * 超时属于「执行状态未确认」：在对应 endpoint 累计 unconfirmed_timeouts；
        * 只有现地址的失败才刷新节点健康标志；旧址的任何结论都不得覆盖新址状态。
        """
        async with self.cond:
            if unconfirmed:
                endpoint.unconfirmed_timeouts += 1
            if endpoint is node.current:
                endpoint.healthy = False
                endpoint.last_probe_ok = False
                endpoint.last_probe_detail = detail
                endpoint.last_probe_at = time.time()
            node.last_change_at = time.time()
            self.cond.notify_all()

    async def probe_url(self, url: str) -> tuple[bool, str]:
        """对指定 URL 做一次探测，只返回结论，不写任何节点状态。"""
        assert self.client is not None
        try:
            resp = await self.client.get(f"{url}/", timeout=PROBE_TIMEOUT)
            return resp.status_code < 500, f"HTTP {resp.status_code}"
        except httpx.TimeoutException as exc:
            return False, f"探测超时: {type(exc).__name__}"
        except httpx.RequestError as exc:
            return False, f"探测连接错误: {type(exc).__name__}: {str(exc)[:100]}"

    async def probe_one(self, node: NodeState) -> None:
        """对节点的【现地址】做主动探测，带代际守卫。

        探测发出前记录 generation；返回后若节点已发生替换（代际变化或现地址已换），
        结论一律丢弃——旧址探测结论永不覆盖新址状态。探测成功只置 healthy=True，
        绝不解除 draining。
        """
        endpoint = node.current
        url = endpoint.url
        generation = node.generation
        ok, detail = await self.probe_url(url)

        async with self.cond:
            if node.generation != generation or node.current is not endpoint:
                node.stale_probe_skips += 1
                return
            endpoint.healthy = ok
            # 注意：这里刻意不修改 node.draining
            endpoint.last_probe_at = time.time()
            endpoint.last_probe_ok = ok
            endpoint.last_probe_detail = detail
            node.last_change_at = time.time()

    async def probe_all(self) -> None:
        # 只探测现地址；旧址不再被主动探测，其健康状态保持冻结
        await asyncio.gather(*(self.probe_one(n) for n in self.nodes))

    async def _probe_loop(self) -> None:
        while True:
            try:
                await self.probe_all()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # 探测循环本身不能崩
                print(f"[probe-loop] unexpected error: {exc!r}")
            await asyncio.sleep(PROBE_INTERVAL)

    async def replace_address(
        self, node: NodeState, new_url: str
    ) -> tuple[int, dict]:
        """单节点热替换。返回 (HTTP 状态码, 响应体)。

        规则：
          * 同节点并发替换只允许一轮，持锁失败 -> 409；
          * 旧址仍有在途 / 未确认超时 -> 409，防止多代旧址堆叠丢失记账；
          * 先探测新址，失败 -> 502 且原地址与接流状态原样保留；
          * 成功后旧 current 转为 previous（在途请求继续钉住它），新址成为 current。
        """
        lock = self._replace_locks[node.name]
        if lock.locked():
            return 409, {
                "error": "replace_in_progress",
                "node": node.name,
                "detail": "同一节点已有替换在进行，并发替换只允许一轮生效",
            }
        async with lock:
            async with self.cond:
                if node.previous is not None and not node.previous.offline_safe():
                    prev = node.previous
                    return 409, {
                        "error": "previous_not_settled",
                        "node": node.name,
                        "detail": (
                            "旧址仍有在途请求或未确认超时，不能发起新一轮替换；"
                            "待其结束并经 replace/confirm 确认后再试"
                        ),
                        "old_address": {
                            "url": prev.url,
                            "inflight": prev.inflight,
                            "unconfirmed_timeouts": prev.unconfirmed_timeouts,
                        },
                    }
                old_url = node.current.url
            if new_url == old_url:
                return 400, {
                    "error": "same_as_current",
                    "node": node.name,
                    "base_url": old_url,
                    "detail": "新地址与当前地址相同，无需替换",
                }

            # 先探测新址；探测期间不改动任何节点状态
            ok, detail = await self.probe_url(new_url)
            if not ok:
                async with self.cond:
                    snap = node.snapshot()
                return 502, {
                    "error": "probe_failed",
                    "node": node.name,
                    "replaced": False,
                    "probe_ok": False,
                    "probe_detail": detail,
                    "base_url": snap["base_url"],
                    "eligible": snap["eligible"],
                    "detail": "新址探测失败，原地址与接流状态保持不变",
                }

            async with self.cond:
                old_endpoint = node.current
                now = time.time()
                old_endpoint.retired_at = now
                new_endpoint = EndpointInfo(
                    url=new_url,
                    healthy=True,
                    last_probe_at=now,
                    last_probe_ok=True,
                    last_probe_detail=detail,
                )
                # 已结清的历史旧址直接被新的 previous 覆盖（记账已确认完毕）
                node.previous = old_endpoint
                node.current = new_endpoint
                node.generation += 1
                node.last_change_at = now
                self.cond.notify_all()
                snap = node.snapshot()
            return 200, {
                "node": node.name,
                "replaced": True,
                "previous_url": old_url,
                "base_url": new_url,
                "generation": snap["generation"],
                "probe_ok": True,
                "probe_detail": detail,
                "eligible": snap["eligible"],
                "offline_safe": snap["offline_safe"],
                "old_address": snap["old_address"],
                "detail": "新址探测成功并已接流；旧址在途请求仍取回原响应",
            }

    async def confirm_previous(self, node: NodeState) -> tuple[int, dict]:
        """人工确认旧址：清零未确认超时计数；无在途后旧址即报告可下线。"""
        async with self.cond:
            prev = node.previous
            if prev is None:
                return 404, {
                    "error": "no_previous_address",
                    "node": node.name,
                    "detail": "该节点当前没有待确认的旧址",
                }
            if prev.inflight > 0:
                return 409, {
                    "error": "previous_still_inflight",
                    "node": node.name,
                    "old_address": {
                        "url": prev.url,
                        "inflight": prev.inflight,
                        "unconfirmed_timeouts": prev.unconfirmed_timeouts,
                    },
                    "detail": "旧址仍有在途请求，不能确认下线",
                }
            prev.unconfirmed_timeouts = 0
            prev.confirmed_at = time.time()
            snap = node.snapshot()
        return 200, {
            "node": node.name,
            "confirmed": True,
            "offline_safe": snap["offline_safe"],
            "old_address": snap["old_address"],
            "detail": "旧址未确认项已清零，状态接口现可报告可下线",
        }

    async def drain(
        self, node: NodeState, timeout: Optional[float]
    ) -> tuple[bool, int]:
        """置排空标志并等待在途清零（含旧址在途）。返回 (是否排空完成, 剩余在途数)。"""
        loop = asyncio.get_running_loop()
        deadline = None if timeout is None else loop.time() + max(timeout, 0.0)
        async with self.cond:
            node.draining = True
            node.last_change_at = time.time()
            # inflight 属性聚合现地址 + 旧址，排空回执因此必然考虑旧址未结束的请求
            while node.inflight > 0:
                if deadline is None:
                    await self.cond.wait()
                    continue
                remaining = deadline - loop.time()
                if remaining <= 0:
                    return False, node.inflight
                # asyncio.Condition.wait 不支持 timeout，用 wait_for 限时等待
                try:
                    await asyncio.wait_for(self.cond.wait(), timeout=remaining)
                except asyncio.TimeoutError:
                    pass  # 回到循环重新计算剩余时间 / 在途数
            return True, 0

    async def resume(self, node: NodeState) -> None:
        """恢复接流：先清排空并保守置为不健康，再立即探测现地址，由探测决定能否接流。"""
        async with self.cond:
            node.draining = False
            # 恢复不绕过故障探测：探测结论出来前一律不接流，杜绝状态窗口
            node.current.healthy = False
            node.current.last_probe_ok = None
            node.current.last_probe_detail = "恢复接流，等待首次探测确认"
            node.last_change_at = time.time()
        await self.probe_one(node)


# ----------------------------- 应用生命周期 -----------------------------

def _build_nodes() -> list[NodeState]:
    return [
        NodeState(name=name, current=EndpointInfo(url=url))
        for name, url in _parse_upstreams(UPSTREAMS_RAW)
    ]


gateway = Gateway(_build_nodes())


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with httpx.AsyncClient() as client:
        gateway.client = client
        # 启动先做一轮探测，避免把流量打到未知状态的节点
        await gateway.probe_all()
        gateway._probe_task = asyncio.create_task(gateway._probe_loop())
        try:
            yield
        finally:
            if gateway._probe_task:
                gateway._probe_task.cancel()
                try:
                    await gateway._probe_task
                except asyncio.CancelledError:
                    pass


app = FastAPI(
    title="http-entry-gateway",
    version="1.1.0",
    description=(
        "仅转发 GET 的入口网关：轮询健康且未排空的上游，支持排空/恢复/状态管理，"
        "支持单节点地址热替换（先探测后切换、旧请求钉住旧址、旧址结清前不可下线），"
        "无可接流节点时返回 503。"
    ),
    lifespan=lifespan,
)


# ----------------------------- 转发接口 -----------------------------

@app.get("/", tags=["meta"])
async def root() -> dict:
    return {
        "service": "http-entry-gateway",
        "proxy": "GET /proxy/{path} 仅支持 GET",
        "admin": [
            "GET  /admin/status",
            "POST /admin/nodes/{name}/drain?timeout=",
            "POST /admin/nodes/{name}/resume",
            "POST /admin/nodes/{name}/replace",
            "POST /admin/nodes/{name}/replace/confirm",
            "POST /admin/probe",
        ],
    }


@app.get("/proxy/{path:path}", tags=["proxy"])
async def proxy(path: str, request: Request) -> Response:
    node, endpoint = await gateway.pick_node()
    if node is None:
        return JSONResponse(
            status_code=503,
            content={
                "error": "no_eligible_upstream",
                "detail": "当前没有健康且未排空的上游节点可接流",
            },
        )

    # 整轮请求钉住选中的 endpoint：热替换只改变后续新请求的去向
    target_url = f"{endpoint.url}/{path}"
    ok = False
    try:
        try:
            upstream_resp = await gateway.client.request(
                "GET",
                target_url,
                params=request.query_params,
                timeout=REQUEST_TIMEOUT,
            )
        except httpx.TimeoutException as exc:
            # 超时：执行结果未确认，累计到该地址；旧址超时不覆盖新址健康状态
            await gateway.mark_endpoint_failure(
                node, endpoint, f"请求超时 {type(exc).__name__}", unconfirmed=True
            )
            return JSONResponse(
                status_code=504,
                content={"error": "upstream_timeout", "node": node.name,
                         "upstream_url": endpoint.url},
            )
        except httpx.RequestError as exc:
            await gateway.mark_endpoint_failure(
                node, endpoint, f"{type(exc).__name__}: {str(exc)[:100]}"
            )
            return JSONResponse(
                status_code=502,
                content={"error": "upstream_unreachable", "node": node.name,
                         "upstream_url": endpoint.url},
            )

        # 5xx 视为上游故障：如实透传状态码并降级，绝不虚报成功
        if upstream_resp.status_code >= 500:
            await gateway.mark_endpoint_failure(
                node, endpoint, f"上游返回 HTTP {upstream_resp.status_code}"
            )

        ok = upstream_resp.status_code < 500
        headers = {
            k: v
            for k, v in upstream_resp.headers.items()
            if k.lower() not in _HOP_BY_HOP
        }
        headers["X-Gateway"] = "http-entry-gateway"
        headers["X-Upstream-Node"] = node.name
        headers["X-Upstream-Url"] = endpoint.url
        return Response(
            content=upstream_resp.content,
            status_code=upstream_resp.status_code,
            headers=headers,
            media_type=upstream_resp.headers.get("content-type"),
        )
    finally:
        # 无论成功、失败还是取消，在途计数都必须准确归还到被钉住的 endpoint
        await gateway.release_node(node, endpoint, ok)


# 非 GET 方法访问转发路径：FastAPI 自动返回 405 Method Not Allowed


# ----------------------------- 管理接口 -----------------------------

@app.get("/admin/status", tags=["admin"])
async def admin_status() -> dict:
    async with gateway.cond:
        return {
            "probe_interval": PROBE_INTERVAL,
            "request_timeout": REQUEST_TIMEOUT,
            "nodes": [n.snapshot() for n in gateway.nodes],
            "eligible_nodes": [n.name for n in gateway.nodes if n.can_serve()],
        }


@app.post("/admin/nodes/{name}/drain", tags=["admin"])
async def admin_drain(name: str, timeout: Optional[float] = DRAIN_DEFAULT_TIMEOUT):
    """排空节点：立即停止分配新请求（含替换后的新址），并阻塞等待在途清零。

    在途统计聚合现地址与旧址，只有全部归零才返回 drained=true；超时仍有在途
    （含旧址在途）时返回 202，drained=false 且节点继续保持排空状态。回执同时
    给出旧址状态与 offline_safe，未确认超时未清前不得报告可下线。
    """
    node = gateway.get(name)
    if node is None:
        return JSONResponse(status_code=404, content={"error": "unknown_node", "name": name})

    drained, inflight = await gateway.drain(node, timeout)
    async with gateway.cond:
        snap = node.snapshot()
    body = {
        "node": name,
        "draining": True,
        "drained": drained,
        "inflight": inflight,
        "offline_safe": snap["offline_safe"],
        "old_address": snap["old_address"],
        "detail": (
            "在途请求（含旧址）已全部结束"
            if drained
            else "等待在途请求结束超时，节点仍处于排空中"
        ),
    }
    return JSONResponse(status_code=200 if drained else 202, content=body)


@app.post("/admin/nodes/{name}/resume", tags=["admin"])
async def admin_resume(name: str):
    node = gateway.get(name)
    if node is None:
        return JSONResponse(status_code=404, content={"error": "unknown_node", "name": name})

    await gateway.resume(node)
    snap = node.snapshot()
    return {
        "node": name,
        "draining": False,
        "base_url": snap["base_url"],
        "healthy": snap["healthy"],
        "eligible": snap["eligible"],
        "last_probe_detail": snap["last_probe_detail"],
        "detail": "已解除排空；是否可接流以现地址健康探测结论为准",
    }


@app.post("/admin/nodes/{name}/replace", tags=["admin"])
async def admin_replace(name: str, body: ReplaceRequest):
    """单节点地址热替换。

    提交新 URL 后先探测新址：探测失败返回 502 且原地址 / 接流状态原样保留；
    探测成功则新 GET 立即转到新址，已在途请求继续访问旧址并取回原响应。
    同节点并发替换仅一轮生效（409）；旧址未结清时拒绝新一轮替换（409）。
    """
    node = gateway.get(name)
    if node is None:
        return JSONResponse(status_code=404, content={"error": "unknown_node", "name": name})
    try:
        new_url = _normalize_url(body.new_url)
    except ValueError as exc:
        return JSONResponse(
            status_code=400,
            content={"error": "invalid_url", "node": name, "new_url": body.new_url,
                     "detail": str(exc)},
        )

    status_code, payload = await gateway.replace_address(node, new_url)
    return JSONResponse(status_code=status_code, content=payload)


@app.post("/admin/nodes/{name}/replace/confirm", tags=["admin"])
async def admin_replace_confirm(name: str):
    """确认旧址执行状态：清零未确认超时；之后状态接口方可报告该节点可下线。"""
    node = gateway.get(name)
    if node is None:
        return JSONResponse(status_code=404, content={"error": "unknown_node", "name": name})
    status_code, payload = await gateway.confirm_previous(node)
    return JSONResponse(status_code=status_code, content=payload)


@app.post("/admin/probe", tags=["admin"])
async def admin_probe() -> dict:
    """手动触发一轮探测（只探现地址，探测不会改变任何节点的排空标志）。"""
    await gateway.probe_all()
    async with gateway.cond:
        return {"nodes": [n.snapshot() for n in gateway.nodes]}
