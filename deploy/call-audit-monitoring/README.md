# Call Audit 死信监控

本目录提供 VM101 上的只读采集器、每分钟 systemd timer 和 Prometheus 规则。数据流为：

`本机 admin health → 原子 textfile → 现有 node_exporter → Prometheus → 现有 Alertmanager / WeCom`

安装这些文件不需要变更应用 Compose、Redis、Audit Query、Nginx、备用容器或 Alertmanager 路由。本目录不包含通知发送器或通知测试。提交源码不等于安装或激活；生产安装应作为单独授权的窄范围部署进行。

## 采集边界

- 只调用 `/usr/bin/docker --host unix:///var/run/docker.sock inspect --type container sub2api`，确认唯一目标正在运行。子进程使用固定最小环境，忽略 `DOCKER_HOST`、`DOCKER_CONTEXT`、TLS 与用户 Docker 配置。
- `ADMIN_API_KEY` 只存在于进程内存和本机 HTTP 请求头，不写配置、参数、指标或日志。Docker 完整 inspect 输出和 health 正文均不打印；错误日志只有固定原因码。不会输出用户、请求、对象键或错误正文标签。
- URL 固定为 VM101 Docker 桥地址上的 `http://172.17.0.1:18081/api/v1/admin/call-audit/health`。忽略 HTTP 代理并拒绝所有重定向，不能通过 CLI 指定其他地址。部署前核对该地址仍属于 VM101 本机，仍映射到主容器。
- HTTP **200 和 503 都读取 JSON**。启用但未就绪的快照应为 503，采集器仍成功发布 `ready=0` 及死信数量。认证失败、其他状态码、超时、非 JSON、超长响应、缺字段或错误类型均使采集失败。
- 只导出白名单布尔值和非负整数。布尔值不接受 `0/1`，整数不接受布尔值、浮点数或字符串。拒绝重复 JSON 键、非标准数值、半套新 schema 和不一致的 readiness/status。
- 兼容完整旧快照：发布已知旧指标和 `recovery_metrics_available=0`，**不把缺失的新字段补零**；专门规则持续 5 分钟后提示升级。缺失旧字段或部分新字段视为失败。
- 采集失败时原子替换文件，只留下 `collector_success=0`、本次时间及上次成功时间，旧 `ready=1` 或 `dead_letter_files=0` 不会残留。磁盘无法写入、进程被杀或 timer 停止时，独立的 absent/stale 规则兜底。
- Docker 查询及 HTTP socket 超时各为 5 秒；service 总时限 20 秒。进程退出后不保留密钥。root/Docker socket 本身有高权限，systemd 硬化不把 Docker API 变成只读 API；因此脚本及 unit 必须由 root 管理、不可由应用账户写入。

输出默认是 `/var/lib/node_exporter/textfile_collector/sub2api_call_audit.prom`；临时文件不以 `.prom` 结尾，写完 fsync 后同目录 rename，最终权限 0644。`--output` 仅供本地测试或安装时验证输出路径，不改变采集目的地。

## 指标与告警

所有指标以 `sub2api_call_audit_` 开头，无应用维度标签。`job`、`instance` 来自现有 Prometheus node scrape。规则当前固定匹配 `job="node", instance="basic-services"`；更换机器或 scrape label 时应一起修改规则和测试，不能只改其中一个。

| 告警 | 条件 | 等待 | 级别 |
|---|---|---|---|
| `Sub2APICallAuditDeadLetters` | 死信文件数 > 0，尚无耗尽/永久/一小时升级条件 | 5 分钟 | warning |
| `Sub2APICallAuditRetryExhausted` | 耗尽或永久失败数 > 0 | 1 分钟 | critical |
| `Sub2APICallAuditDeadLetterTooOld` | 最老未解决死信年龄 ≥ 3600 秒 | 1 分钟 | critical |
| `Sub2APICallAuditCaptureFailures` | 10 分钟窗口内 capture failures 增长 | 无额外等待 | critical |
| `Sub2APICallAuditBacklogTooOld` | backlog > 0 且最老待处理 ≥ 300 秒 | 5 分钟 | warning |
| `Sub2APICallAuditCollectorUnavailable` | 采集失败、指标缺失或成功时间超过 180 秒 | 2 分钟 | critical |
| `Sub2APICallAuditRecoveryMetricsMissing` | 启用审计但仅有旧恢复 schema | 5 分钟 | warning |
| `Sub2APICallAuditNotReady` | 审计禁用或未就绪，且无死信/恢复中条目 | 5 分钟 | warning |
| `Sub2APICallAuditAutoRetryDisabled` | 审计启用但自动重试禁用 | 5 分钟 | warning |

恢复字段包括 `dead_letter_recovering/retryable/exhausted/permanent`、`oldest_dead_letter_seconds`、`dead_letter_auto_retry_enabled`、`dead_letter_retries_total`、`dead_letter_recovered_total`。基础计数器加 `_total` 后导出。应用计数器会随重启重置，告警使用 `increase()` 处理 reset。每分钟快照不能保证观察到进程重启前的所有瞬时计数变化；这不是持久化审计事件源。

失败和恢复通知继续使用已配置的 Alertmanager 分组、重复间隔和 WeCom 路由。不要为本目录另建接收人或 webhook；不要用真实死信或合成 production alert 验证通知。

死信进入 critical 条件后抑制一般死信 warning；死信及恢复中的降级由专门告警负责，避免同批条目再触发 NotReady。旧 schema 没有新分类指标时仍保留基本死信 warning。critical 条件消失但死信仍存在时，warning 重新计算 5 分钟等待期。

## 本地验证

从仓库根目录执行。Python 测试仅用标准库和模拟输入，不访问 Docker 或网络；Promtool 容器也禁用网络。

```bash
python3 -m unittest discover -s deploy/call-audit-monitoring/tests

docker run --rm --network none --entrypoint /bin/promtool \
  -v "$PWD/deploy/call-audit-monitoring:/work:ro" -w /work \
  prom/prometheus:v3.5.0@sha256:63805ebb8d2b3920190daf1cb14a60871b16fd38bed42b857a3182bc621f4996 \
  check rules sub2api-call-audit.rules.yml

docker run --rm --network none --entrypoint /bin/promtool \
  -v "$PWD/deploy/call-audit-monitoring:/work:ro" -w /work \
  prom/prometheus:v3.5.0@sha256:63805ebb8d2b3920190daf1cb14a60871b16fd38bed42b857a3182bc621f4996 \
  test rules tests/rules.test.yml
```

测试包含 503 采样、白名单和类型、密钥保密、代理/重定向拒绝、失败时旧指标清除、原子发布、旧/部分 schema，以及告警边界、计数器 reset、缺失和 stale。

## 经授权后窄范围安装

先保存将覆盖的三个 collector 文件和单一规则文件的备份，记录 timer 原有状态以及 SHA256。由 Ansible 托管时，应在干净隔离的 HA checkout 中增加独立任务，只负责这些文件；不要直接修改 HA 的脏工作区，也不要运行完整监控或业务部署 playbook。

### VM101

前提：`/usr/bin/python3`（Python ≥ 3.9）、`/usr/bin/docker`、`/var/run/docker.sock` 可用；textfile 目录已存在，规范权限是 `root:node_exporter 0755`；现有 node_exporter 参数已包含该目录。采集器需要 root 才能读取 Docker socket；空 capability bounding set 与上述 root 所有目录兼容。不要通过放宽 socket 权限或给应用用户 docker 组来适配。

把本目录四个源文件复制到受控 staging 目录，先执行本地验证，再在 VM101 staging 目录下执行以下已审阅命令。规则文件在下一步安装到 Prometheus 主机。

```bash
sudo test -d /var/lib/node_exporter/textfile_collector
sudo stat -c '%U:%G %a' /var/lib/node_exporter/textfile_collector
sudo systemctl show node_exporter --property=ExecStart --value
sudo install -d -o root -g root -m 0755 /usr/local/libexec
sudo install -o root -g root -m 0755 collector.py /usr/local/libexec/sub2api-call-audit-collector.py
sudo install -o root -g root -m 0644 sub2api-call-audit-collector.service /etc/systemd/system/
sudo install -o root -g root -m 0644 sub2api-call-audit-collector.timer /etc/systemd/system/
sudo systemd-analyze verify /etc/systemd/system/sub2api-call-audit-collector.service /etc/systemd/system/sub2api-call-audit-collector.timer
sudo systemctl daemon-reload
sudo systemctl start sub2api-call-audit-collector.service
sudo systemctl show sub2api-call-audit-collector.service --property=Result --property=ExecMainStatus
sudo systemctl enable --now sub2api-call-audit-collector.timer
```

检查 `/var/lib/node_exporter/textfile_collector/sub2api_call_audit.prom` 内容仅有白名单指标；等待至少两个自然周期，确认 `collector_last_success_unixtime` 前进、`collector_success=1`，以及恢复 schema 可用。503 降级仍可采集；这时应检查真实审计状态，不能用 `/health` 或服务退出码代替审计验证。无需重启 node_exporter。

### Prometheus 主机

前提：现有配置的 `rule_files` 已匹配 `/etc/prometheus/rules/*.yml`，`up{job="node",instance="basic-services"}=1`，且新指标已被该 target 抓取。先安装并验证 collector，再启用规则，避免由安装顺序制造缺失告警。

把规则放到 staging，确认当前 Prometheus 的 promtool 也校验通过后，只新增或替换一个规则文件：

```bash
promtool check rules sub2api-call-audit.rules.yml
sudo install -o root -g root -m 0644 sub2api-call-audit.rules.yml /etc/prometheus/rules/sub2api-call-audit.yml
sudo promtool check config /etc/prometheus/prometheus.yml
sudo systemctl reload prometheus
```

如果完整配置校验失败，先恢复本次单一规则文件，不执行 reload。读取 Prometheus `/api/v1/rules?type=alert` 确认组 `sub2api-call-audit` 已加载且 evaluation health 正常；查询新指标及 `ALERTS`，核对实际状态。读取既有 Alertmanager 状态即可，不更改 WeCom 路由、不发送测试消息。若规则未来由 HA role 接管，要保留该文件，避免后续收敛时删除。

### 回滚

先只撤回本目录的 Prometheus 规则文件（恢复原件或移除本次新增件），校验配置并 reload，然后停用 collector timer 并等待当前 oneshot 结束，再恢复原 collector/unit 文件或移除本次新增件，执行 daemon-reload。最后移除本次产生的单一 `sub2api_call_audit.prom` 文件。保留其他 textfile 文件、node_exporter 配置和 Alertmanager 路由；不要先停 collector 造成已启用规则持续报告缺失。
