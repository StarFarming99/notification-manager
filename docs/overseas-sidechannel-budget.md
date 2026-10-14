# US/AP/EU 测试旁路预算和撤线

三个通知域沿用同一个 infra-alerts App，只复制指定 critical receiver 原本进入生产群的告警到测试群。原 NM、原 callback 和 PD 不进入新限流器；formal 继续关闭。

2026-10-14 反馈方式更新：准确/不准确在 Console 判断详情页提交，并归入“反馈”页面。Jev 补标组件不再生成准确性 action reference，NM/执行器即使收到历史引用也不渲染这两个按钮。Oncall/Pool 补标、详情链接和原确认/拉群/屏蔽元素保留；共享 callback 无需升级。用户已确认本地 mock，授权重新构建并上传 NM/执行器镜像；本轮只发布镜像和更新部署步骤，尚未授权部署。

## 固定预算

| 路径 | 各域配置 | 覆盖范围 |
|---|---|---|
| 新 NM 基础卡 | `NM_NOTIFICATION_SEND_INTERVAL=12s` | 每域全部工作线程、历史 test profile、持久重试共用账本时钟 |
| executor PATCH | `JEV_EXECUTOR_PATCH_INTERVAL=12s` | 每域全部已安装 scope 和 PATCH 尝试（含明确失败后的重试）共用持久时钟 |
| 同卡 PATCH | 最少 15 秒 | 同时执行 Jev 的 `X-Jev-Min-Update-Interval-Seconds`，取更严格值；合法值为 1–3600 秒 |

三个域新基础卡上限合计 0.25 次/秒，PATCH 上限合计 0.25 次/秒，稳态合计不超过 0.5 次/秒。各域独立时钟可能同时发生首次调用，静态分配不宣称消除短暂三域同步突发。Token 查询不是发卡/PATCH 额度；原 App 已有请求仍需单独观测配额。

两类额度在外部调用前持久化；重启沿用 `next_attempt_at`，不恢复初始突发额度。NM 的默认发送间隔仍为 2 秒，必须在海外部署包显式填写 12 秒。没有 profile 的原通知不被新增额度限制。

离线 `notification-spool-recovery --action replay-target` 同样执行持久发送边界和额度；带 profile 的目标必须提供原 delivery profiles 与相同发送预算配置，缺配置保持未发送并返回可重试状态。恢复工具不能通过直接调用平台绕过暂停或把明确未发送标记保留在实际已发出的记录中。

executor 在预算不足时立即返回 `429` 和 `Retry-After`，不等待 12/15 秒、不写一个假 `sending` intent。调用方保留同一幂等键和组件，按重试时间重新提交。已确认幂等请求直接返回原 request ID，不再调用飞书，不消耗新额度。PATCH HTTP 请求最多执行 2 秒；结果不确定仍保留 unknown 及原 intent，禁止盲重放。

## 每域暂停

NM 沿既有控制入口：

1. 读取 `GET /internal/delivery-profiles` 的 test revision。
2. 使用独立 profile control credential 执行 `POST /internal/delivery-profiles/test/pause`，填写 CAS revision、actor 和 evidence。
3. 准入与领取停止。已经领取但尚未经过发送边界的工作不得继续发卡，保留同一个 frozen target 重试。

`pause` 是有界在途发送的完成屏障：已进入外部调用的工作先完成后控制返回；控制返回后没有新的平台调用。发送前领取使用可选 `send_prepared` 标记，新进程只会安全重排这些明确未发送的 profile 工作；旧格式/已进入发送边界的 `sending` 仍恢复为 unknown。不会把已接受工作改发生产群。

executor 使用独立 `JEV_EXECUTOR_WRITER_CONTROL_TOKEN`（至少 32 字节，与 relay、receipt、annotation、feedback 凭据均不同）：

```http
GET /internal/jev/writer-control
Authorization: Bearer <writer-control-token>
```

响应提供 `state.revision`、`state.paused`、持久预算时钟、当前 `permission_domain` 和 `execution_domain`。暂停与恢复：

```json
{
  "action": "pause",
  "expected_revision": 1,
  "permission_domain": "infra-ap",
  "actor": "operator",
  "evidence": "withdraw AP test lane"
}
```

向 `POST /internal/jev/writer-control` 发送以上对象；恢复使用 `action=resume` 和重新读取的 revision。错误域、陈旧 revision、混用业务凭据被拒绝。暂停覆盖本域所有历史 scope，并等待已开始 PATCH 完成；随后 PATCH 返回 `503/Retry-After: 5`。成功基础卡的收据仍可持久接收，避免丢弃撤线前已经发送的在途卡。

完整撤线先移除本域 AM sibling/receiver 并热加载，再暂停本域 NM test 和 executor writer，并暂停中央 Jev 的本域消费/试跑。对账在途任务和 unknown 后保留 PVC、profiles、凭据和收据；不回滚整库，不恢复旧账本，不修改其他域。这里只实现 NM/executor 边界，中央消费暂停由 Jev 配套功能执行。

`writer-control.json` 单独存放在原 card-state 根目录，所有本域 scope 共用；bindings 格式没有增加字段。旧 executor 不解析该文件，组件回滚不需要重写卡片或恢复历史快照。回滚旧镜像会丢失这些新暂停/配额执行能力，因此撤线和调用方暂停应先完成。

## 本地验证

新增测试覆盖：跨 worker 预发送预算、重启预算不重置、未发送 claim 安全恢复、pause 阻止已领工作且等待在途发送、原通知不限速；PATCH 同 MID/header、不同 MID 全局额度、成功幂等回放、明确失败重试、重启与暂停 CAS。既有通知渠道、formal 兼容、原按钮渲染及收据测试继续运行。生产 App 配额、真实同卡 PATCH 和原按钮交互仍属于部署后自然告警验收。
