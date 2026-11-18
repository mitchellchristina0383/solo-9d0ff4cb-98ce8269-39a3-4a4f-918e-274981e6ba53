# HTTP 入口网关（支持优雅排空 / 健康探测 / 轮询转发 / 单节点地址热替换）

滚动更新场景下的入口网关：运维人员先排空待停机实例，使新请求不再进入，
同时保证在途长请求完整取回原响应，全部结束后才报告排空完成。

值班员替换上游实例地址时，可调用**单节点地址热替换 API**：先探测新址，探测失败
则原地址与接流状态原样保留；探测成功后新 GET 立即转到新址，已在途请求仍钉住旧址
取回原响应；旧址仍有在途请求或超时未确认项时，状态接口与排空回执都不报告可下线。

技术栈：**FastAPI + httpx**，由 **Docker Compose** 启动 1 个网关 + 3 个可模拟
慢请求 / 故障的上游（`upstream3` 默认作为热替换目标，不在初始上游列表中）。

## 目录结构

```
.
├── docker-compose.yml      # 编排：gateway + upstream1/2/3
├── gateway/
│   ├── app.py              # 网关：仅转发 GET、轮询、探测、排空/恢复/热替换/状态
│   ├── requirements.txt
│   └── Dockerfile
├── upstream/
│   ├── app.py              # 模拟上游：slow / flaky / 故障模式控制
│   ├── requirements.txt
│   └── Dockerfile
└── tests/
    └── self_test.py        # 本地端到端自测（22 组场景，98 项断言）
```

## 一键启动

```bash
docker compose up --build
# 网关:        http://localhost:18080
# 上游 node1:  http://localhost:18081
# 上游 node2:  http://localhost:18082
# 替换目标:    http://localhost:18083
# 交互文档:    http://localhost:18080/docs
```

网关环境变量（见 `docker-compose.yml`）：

| 变量 | 默认 | 含义 |
| --- | --- | --- |
| `UPSTREAMS` | `node1=http://upstream1:8000,node2=http://upstream2:8000` | 上游列表 `name=url` |
| `PROBE_INTERVAL` | `2.0` | 周期探测间隔（秒） |
| `PROBE_TIMEOUT` | `1.5` | 探测超时（秒） |
| `REQUEST_TIMEOUT` | `15.0` | 转发请求超时（秒） |
| `DRAIN_DEFAULT_TIMEOUT` | `30.0` | 排空 API 默认等待上限（秒） |

## API 一览（均可在 /docs 直接操作）

转发（仅 GET）：

```
GET /proxy/{path}      # 轮询转发到健康且未排空的节点，透传查询串与响应
                       # 无可用节点 -> 503；上游超时 -> 504；连不上 -> 502
                       # 非 GET 方法 -> 405
```

管理：

```
GET  /admin/status                       # 全部节点健康/排空/在途/计数/最近探测
                                        # 含 current 现址、previous 旧址、
                                        # old_address 与 offline_safe
POST /admin/nodes/{name}/drain?timeout=# 排空：立即停接新流，阻塞到在途清零
                                        #   在途归零 -> 200 drained=true
                                        #   超时仍有在途 -> 202 drained=false
                                        # 回执含 old_address 与 offline_safe
POST /admin/nodes/{name}/resume        # 恢复：清排空，立即探测，由探测决定能否接流
POST /admin/nodes/{name}/replace       # 热替换：body {"new_url": "http://..."}
POST /admin/nodes/{name}/replace/confirm  # 人工确认旧址：清零未确认超时
POST /admin/probe                      # 手动触发一轮探测（只探现址）
```

### 热替换语义（`POST /admin/nodes/{name}/replace`）

1. **先探测后切换**：对 `new_url` 发 `GET /` 探测（超时 / 连接错误 / 5xx 均视为失败）。
   探测期间不改动任何节点状态。
2. **探测失败 → 502 `probe_failed`**：`base_url`、健康位、排空位与接流状态全部原样保留，
   新请求仍走原地址；非法 URL → 400，与现址相同 → 400，未知节点 → 404。
3. **探测成功 → 200**：旧址转为 `previous` 并冻结探测状态，新址成为 `current` 立即接流；
   节点 `generation` 加 1。已在途请求钉住选中时的 endpoint 对象，**替换后仍访问旧址 URL
   并取回原响应，不被截断**。
4. **可下线判定 `offline_safe`**：仅当旧址 `inflight=0` 且无 `unconfirmed_timeouts`
   （请求超时后执行状态未确认的计数）时才为 true。状态接口与排空回执都引用同一判定，
   未确认项需值班员调用 `replace/confirm` 清零（有在途时 confirm 返回 409）。
5. **并发与串行**：每节点一把替换锁，并发替换只有一轮生效，后来者得到 409
   `replace_in_progress`；旧址未结清（仍有在途或未确认超时）时发起新一轮替换得到
   409 `previous_not_settled`，避免多代旧址堆叠丢失记账。
6. **代际守卫**：所有探测（周期 / 手动 / resume）发出前记录 `generation`，返回时若节点
   已替换，则结论丢弃并累计 `stale_probe_skips`——**旧址探测结论永不覆盖新址状态**。
   周期探测只探现址；旧址上的请求失败只记旧址的账，不刷新新址健康位。
7. **与排空正交**：排空中替换地址，节点依旧不接流（替换不解除排空），新址同样不接流；
   排空等待的在途数聚合现址 + 旧址，因此**排空回执必然考虑旧址未结束的请求**。

上游模拟控制（演练用，直连上游端口，不经过网关）：

```
GET  /control/state            # 查看模式与在途数
POST /control/mode?mode=       # healthy | slow | error | hang
GET  /slow?seconds=N           # 慢请求（验证在途不被截断）
GET  /flaky                    # 按模式返回 200 / 500 / 挂起
GET  /                         # 健康端点（error->503, hang->挂起）
```

## 典型演练

```bash
# 1. 发一个慢请求（10 秒），在另一终端立刻排空 node2
curl "http://localhost:18080/proxy/slow?seconds=10" &
curl -X POST "http://localhost:18080/admin/nodes/node2/drain?timeout=30"
# 现象：drain 请求阻塞，约 10 秒后才返回 drained=true；慢请求拿到完整 200

# 2. 让 node1 进入故障模式，观察探测自动摘除
curl -X POST "http://localhost:18081/control/mode?mode=error"
sleep 3
curl http://localhost:18080/admin/status      # node1 healthy=false

# 3. 故障未消除时恢复 node1：不会绕过探测，节点仍不可接流
curl -X POST http://localhost:18080/admin/nodes/node1/resume
# eligible=false；上游修好后经周期探测自动恢复接流

# 4. 地址热替换到 upstream3（容器内用服务名，宿主机演练可用 http://localhost:18083）
curl -X POST http://localhost:18080/admin/nodes/node1/replace \
  -H 'Content-Type: application/json' -d '{"new_url":"http://upstream3:8000"}'
# 200 replaced=true；之后 GET /proxy/echo 的响应头 X-Upstream-Url 指向新址
curl http://localhost:18080/admin/status
# previous.old_address 记录旧址；inflight 归零且无未确认超时时 offline_safe=true
curl -X POST http://localhost:18080/admin/nodes/node1/replace/confirm

# 5. 慢请求与替换交错：慢请求不被截断，替换期间仍取回旧址响应
curl "http://localhost:18080/proxy/slow?seconds=10" &
curl -X POST http://localhost:18080/admin/nodes/node1/replace \
  -H 'Content-Type: application/json' -d '{"new_url":"http://upstream3:8000"}'

# 6. 探测失败：新址不可达时返回 502，原地址与接流状态保持不变
curl -X POST http://localhost:18080/admin/nodes/node1/replace \
  -H 'Content-Type: application/json' -d '{"new_url":"http://127.0.0.1:9"}'
```

## 关键设计与并发正确性

- **两个独立标志位**：`healthy` 只由探测（含请求失败降级）写入；`draining` 只由
  人工 drain/resume 写入。因此**探测成功永不解除排空**，两者交错互不串改。
- **恢复不绕过故障探测**：`resume` 先清排空并保守置 `healthy=false`，再做一次
  真实探测；探测通过前该节点不会被轮询选中。
- **轮询 + 原子登记**：在同一把 `asyncio.Condition` 锁内完成选节点与
  `inflight += 1`，保证"选中即可接流、计数即生效"。
- **在途计数可靠**：转发用 `try/finally` 归还计数，成功、5xx、超时、连接错误、
  客户端取消都会准确递减；排空等待在条件变量上，请求一结束即被唤醒。
- **排空语义**：置位后新请求立即绕过该节点；已在途请求不被取消，继续等待并
  透传原响应；只有 `inflight` 归零才返回 `drained=true`。
- **不虚报成功**：5xx 如实透传并即时降级；超时返回 504、连接错误返回 502，
  均标记失败并刷新健康状态，绝不返回虚假 200。
- **无节点可接流**：所有节点都不健康或排空中时，`GET /proxy/*` 返回 503。
- **热替换的 endpoint 钉扎**：节点状态拆为 `current` / `previous` 两个 endpoint 对象，
  请求在轮询选中时即钉住具体 endpoint 并在其上计在途；替换只交换节点的指针，
  在途请求整轮仍使用旧址 URL，新请求才走新址。
- **旧址可下线判定**：超时属于「执行状态未确认」，在对应 endpoint 累计
  `unconfirmed_timeouts`；`offline_safe = 旧址 inflight==0 且未确认==0`，
  状态接口与排空回执共用该判定；`replace/confirm` 是清零未确认项的唯一人工入口。
- **替换串行化**：每节点一把 `asyncio.Lock`，锁外先做非阻塞检查，并发替换第二轮
  直接 409；旧址未结清同样 409，从机制上保证同一节点同一时间只有一轮替换生效。
- **代际守卫防状态覆盖**：探测结论回写前校验 `generation` 与 endpoint 身份，过期结论
  丢弃并计入 `stale_probe_skips`；周期探测只探现址，旧址的请求失败只记旧址账目，
  新址健康位不被任何旧址结论改写。

## 自测

```bash
# 无需 docker；需本地具备 fastapi/uvicorn/httpx（可 pip install 到独立目录）
python3 tests/self_test.py
```

覆盖 22 组场景、98 项断言。除既有的探测/排空/超时/在途计数场景外，热替换部分覆盖：
成功替换与流量切换、探测失败与非法输入保留原状、**慢请求与替换交错（旧请求钉住旧址、
未结清不可确认）**、并发替换 200+409、**排空与替换交错（排空中新址不接流、503、
回执含旧址）**、超时未确认导致不可下线与 confirm 放行、**代际守卫丢弃旧址探测结论**。
