# 节点插件接口与装配框架

节点功能通过 `NodePlugin` 接入，注册、开关、依赖校验和生命周期由上层框架统一管理。当前使用同一进程内、编译时注册的 Rust 插件；配置修改在重启后生效。业务实现、配置、路由、工具契约和测试按插件归属放置；仍使用同一个 Rust crate，不动态加载二进制插件。

## 目录与依赖

```text
src/
  app/                     # Shared host State and Application lifecycle.
  core/                    # Authentication, logging, ids, gates and workers.
  config/                  # Deployment parsing and legacy configuration adapter.
  plugin/
    catalog.rs             # Product defaults; the common framework imports no features.
    common/                # Interfaces, configuration view, typed services and lifecycle.
      tests/               # Framework contracts and reusable test support.
      workspace/           # Shared lease and journal primitives.
    files/                 # Directory capabilities, file operations, HTTP and configuration.
    process/               # Command admission, sandbox, supervision and HTTP.
    sync/                  # Sync storage, maintenance, transport and owned tool contracts.
    projects/              # Project state, grants, directory sync integration and tools.
    harness/
      drivers/             # Codex, Claude and DSH native protocols.
    mcp/                   # Generic MCP transport, discovery and resource adaptation.
```

各功能目录含 `plugin.rs`（装配入口）、实现、`contribution.rs`（协议投影）及 `tests/`；有工具的插件持有自己的 `tools.json`。集成测试通过 Cargo 显式 test target 指向这些目录，原 `cargo test --test sync` 等命令继续可用。

State 仅持有认证、目录能力、文件门闩、线程预算、分页密钥、节点 epoch 及通用 `Services`。进程、同步、项目、Harness 和文件操作账本由各插件注册类型化 `Arc<T>`；关闭插件不创建其服务。`State::host` 和 `Application::assemble` 可用空插件目录构建宿主。旧 crate 模块路径通过公开 re-export 保留，代码物理归属以 `plugin/` 为准。

## 内置插件与开关

| ID | 功能 | 未显式配置时 |
| --- | --- | --- |
| `files` | HTTP 文件、导出目录、SeqFile、写入回执 | 开启 |
| `process` | 沙箱探测和 HTTP 命令执行 | 沿用 `execution`，原默认开启 |
| `sync` | 同步服务、HTTP 同步接口和维护任务 | 沿用 `sync.enabled` |
| `projects` | 项目目录与 MCP 项目工具 | 配置 `[projects]` 时开启 |
| `harness` | 原生会话服务和 MCP Harness 工具 | 配置 `[[harnesses]]` 时开启 |
| `mcp` | MCP 传输、发现及目录资源 | 开启 |

完整示例见 [config.plugins.example.toml](../config.plugins.example.toml)。`[plugins]` 中的布尔值优先于旧开关；未知 ID 即使设为 false 也拒绝，避免拼写错误静默放行。开启同步或项目插件必须提供相应配置。

关闭插件后不装配其业务服务、不启动其维护任务，也不挂载其 HTTP 路由。能力发现报告实际开关；关闭文件接口后不再宣称文件读写或 SeqFile 能力，关闭 MCP 后不再宣称远程 Harness 端点。目录句柄、身份认证、授权、文件门闩和工作线程仍属于共享宿主资源，可能同时供项目、命令或 Harness 使用；关闭 `files` 只关闭文件 HTTP 功能，不撤销其他已开启插件所需的目录授权。

节点插件与既有 `HarnessPlugin` 分层：前者装配文件、同步、执行等节点功能；后者处理 Codex、Claude、DSH 等原生驱动。`State::from_config_with_plugins` 仍接受原 Harness 注册表，不改变其既有含义。

## 接口与上层职责

`plugin::common::NodePlugin` 提供稳定 ID、必需和可选依赖、默认开关、有效配置投影、静态验证、服务装配、路由及异步 start/stop。公共接口消费 `PluginConfiguration`，不导入产品 Config；独立插件可使用 `PluginSettings.options` 解析自己的配置。既有部署格式通过 config/plugin_adapter.rs 适配，新增插件的参数可放在 `[plugin_options.<id>]`，无需添加宿主字段。插件只管理自己的功能，资源授权仍由领域模块执行。插件的 stop 必须可重复调用，构造失败时应自行释放局部资源。

`PluginRegistry::empty()` 创建独立注册表，`register(Arc<dyn NodePlugin>)` 加入插件，`PluginRegistry::builtins()` 提供产品默认目录。注册表不使用全局可变状态；重复/非法 ID、缺失或被禁用的必需依赖、依赖环在创建服务之前拒绝。可选依赖仅在双方启用时决定顺序，例如项目在同步库初始化后检查目录隔离，不强制项目开启同步。

`app::Application` 是上层入口：`from_config` 或 `with_registry` 装配 State，`router()` 构建统一认证、CORS、缓存和日志边界，`start().await` 按依赖顺序启动，`shutdown().await` 反序关闭。所有插件路由统一验证请求身份，领域处理器继续检查资源级权限。

插件通过 `McpContribution` 声明工具、可见性、调用、资源读取和错误结果语义；MCP 传输不再导入项目、同步或 Harness，也不按工具名决定调用目标。HTTP/MCP 能力及目录字段同样由插件贡献，公共层检查重名并聚合。原有 44 个工具的名称、参数 Schema 和 annotations 保持一致；收据失败及同步执行的不确定状态由所属插件判断。

启动失败时清理已经尝试启动的插件，包括失败插件自身；单个清理失败不跳过其余插件，原始启动错误和清理错误同时返回。重复 start/shutdown 不重复执行已完成钩子。启动或关闭 future 被取消后，运行时保留尚未清理的插件，后续 shutdown 可以继续；每个 stop 钩子限时 10 秒。各插件在关闭钩子中取消自己的操作，上层最多等待 5 秒排空共享文件门闩，不依赖文件 HTTP 插件是否开启。生命周期日志记录插件、阶段、operationId、耗时、结果和 `PluginFailure.code`，不写入插件参数、配置或错误正文。

## 独立验证

```bash
cargo run --locked -- plugins check config.toml
cargo run --locked -- plugins check config.toml files
cargo run --locked -- plugins check config.toml sync
cargo test --locked --test node_plugins --test plugin_runtime
```

`plugins check` 验证插件开关、依赖及各插件的静态配置，不启动服务、不探测沙箱、不创建存储或日志文件，也不调用模型。指定 ID 时只校验该插件及必需依赖，忽略其他功能的配置错误；明确禁用的必需依赖不会被自动开启。输出仅包含开关和装配顺序。它不代替启动时的目录句柄、凭据、存储健康和沙箱可用性核对。

集成测试覆盖禁用后的路由和能力、独立注册表路由认证、CLI 验证无写入、依赖/环、启动回滚、清理失败及取消后的清理所有权。完整既有功能回归继续使用 `cargo test --locked`；进程沙箱和真实 Harness 验收仍遵循各自环境开关。

## 扩展与验收

### 事件订阅设计（0.4）

`POST /v1/events` 是唯一的节点事件传输入口，使用带认证 header 的 fetch SSE；请求包含版本与最多 32 个 `{id, plugin, args}` 订阅。一个应用运行时按节点与认证身份共享连接，多个项目/profile/进程复用。传输只负责限流、Origin、帧边界、心跳、取消和按订阅 ID 分发；插件通过 `NodePlugin::subscribe` 授权并提供流，不在公共层判断业务工具名。

Harness 直接订阅已有有界事件缓冲，沿用 epoch/cursor/gap 与待答请求快照；满页立即追赶，断线从最后已消费游标恢复，缺口通过权威历史修复。订阅不持有原生 driver，空闲卸载后等待真正的调用重建 driver，不靠心跳启动进程。项目文件通过异步 inotify 就绪通知发送版本失效信号；进程通过状态变更通知发送终态，不以传输断开取消执行。文件正文、同步数据、命令、审批回复和收据继续走原有 HTTP/MCP 操作。

文本事件可小批合并，审批与终态不丢弃。服务端不维护无界发送队列，客户端缓冲溢出明确断开并从游标恢复；慢消费者超出回放窗口时报告 gap。订阅建立和项目变更后都检查资源授权及 revision；错误帧只包含分类和状态，不包含参数、正文或凭据。连接与失败日志记录 operationId、阶段、订阅数量和耗时。

服务端先完成实现与独立测试，再更新客户端接入。接入完成删除旧 `harness_events`、`project_watch`/`project_unwatch` 轮询工具和客户端轮询端口，不保留旧节点回退；部署必须配套升级节点和客户端。快照、回执、取消及用于恢复的进程状态读取保留。

自有插件实现 NodePlugin，按需实现 McpContribution；在独立 PluginRegistry 中注册，再调用 Application::assemble(host, registry, configuration)。类型化状态通过 host.services.insert(Arc::new(OwnState)) 注册，处理器通过 require::<OwnState>() 取得；禁用或未注册时返回能力错误，容器拒绝重复类型，不隐式创建默认服务。

完整扩展示例及验收见 [contributions.rs](../src/plugin/common/tests/contributions.rs)：只注册测试插件和 MCP，即能验证独立配置、状态、HTTP、工具、资源、能力与关闭行为，不初始化其他业务服务。运行 `cargo test --locked --test contributions`。各插件的自测与共享框架测试同时属于 `cargo test --locked`。
