# Linux davfs2 文件入口

WebDAV 是可选的现有文件服务适配器，数据仍在已配置 export 中。原生 `/v1/fs` API 保留；协议入口是 `/dav/<alias>/`，不是项目的虚拟挂载树。启用方式见 [完整示例](../config.webdav.example.toml)：

```toml
[plugin_options.files]
webdav = true
```

默认关闭，关闭时返回 `404 WEBDAV_DISABLED`；files 插件关闭时也不装配入口。配置修改随 Cells 重启生效。不要把此表放进 `[[exports]]` 内。沿用原有 Basic/Bearer 认证、principal/export 写权限、项目声明及生命周期 fence、只读限制、保留路径和 `openat2` 边界；不会通过内部 HTTP 调用，也不会复制一套文件存储。只读 export 可浏览和读取，写入及 LOCK/UNLOCK 返回 `403 EROFS`。

## 安装、凭据与挂载

以下为 Debian/Ubuntu 示例，需安装环境管理员执行；本变更没有在实机上执行挂载、编辑器保存或安装验收。先准备可写 export 目录及已有的 Cells 服务，再选择一个具备该目录授权的 Basic 主体。示例配置的 `CELLS_DAV_PASSWORD` 应通过正式服务的环境/凭据管理注入，不将真实密码写进命令历史或仓库。

```sh
sudo apt-get install davfs2
sudo mkdir -p /mnt/cells-docs
sudo mount -t davfs https://cells.example.org/dav/docs/ /mnt/cells-docs
# 按提示输入该 Cells Basic 主体的用户名和密码。
ls /mnt/cells-docs
sudo umount /mnt/cells-docs
```

本地回环测试可使用 `http://127.0.0.1:8787/dav/docs/`。远程应通过已有 TLS 代理暴露；代理必须保留 DAV 方法、Destination、If、Depth、Timeout、Lock-Token、条件头及操作 ID，不能自动重放写请求。Basic 凭据不应经过明文远程链路。客户端证书、代理认证不能代替 Cells 的主体认证。

自动凭据由执行挂载的用户持有：root 挂载使用 `/etc/davfs2/secrets`，普通用户使用 `~/.davfs2/secrets`。文件权限设为 `0600`，所属目录仅用户可访问。一行采用挂载 URL、用户名、密码（含空格须按 davfs2 规则引用）；不要把示例占位符作为真实凭据。

```text
https://cells.example.org/dav/docs/ dav-user "replace locally"
```

普通用户挂载还需按发行版配置 davfs2 用户组、mount.davfs 权限及 `/etc/fstab` 的 `user,noauto` 条目，重新登录后再使用。root 和普通用户的缓存、配置与 secrets 是不同的；不要混用。Cells 的 Bearer API key 不能直接当作 Basic 密码，需使用已配置的 Basic 主体。

保留 `use_locks 1`，不要为绕过 `423` 关闭锁。davfs2 对文件采用本地缓存及延迟上传，编辑器的保存返回不等于服务器已经提交；确认上传完成并正常卸载后再切换入口。`cache_size`、`delay_upload`、`file_refresh` 等按客户端版本的手册配置，不把缓存当作备份。参考 [mount.davfs 手册](https://manpages.debian.org/trixie/davfs2/mount.davfs.8.en.html) 和 [davfs2.conf 手册](https://manpages.debian.org/trixie/davfs2/davfs2.conf.5.en.html)。

## 协议范围

OPTIONS 的 Allow 列出支持方法，DAV 头提供锁能力发现；此入口实现 davfs2 所需的受限子集，不承诺完整 WebDAV/POSIX 或任意客户端兼容。

| 方法 | 行为和边界 |
| --- | --- |
| OPTIONS | 认证、导出及路径授权后返回能力；关闭配置不能使用。 |
| PROPFIND | 支持 Depth `0`/`1`、空 body/allprop、propname 和命名属性。省略 Depth 或 infinity 返回 `403 DAV_FINITE_DEPTH_REQUIRED`；不递归枚举。单目录最多 10000 项，存在不能表示的条目或扫描告警时明确失败，不返回伪完整列表。 |
| GET/HEAD | 复用原生流式读取、范围和 ETag；HEAD 无正文。If-Match 使用当前 export 版本，If-None-Match 支持 `*` 或单个 ETag。集合不作为文件下载。 |
| PUT | 最多 256 MiB。需要 If-None-Match `*` 创建、单个 If-Match ETag 替换，或当前主体持有且提交了有效锁令牌。无条件且无锁返回 `428 DAV_LOCK_OR_REVISION_REQUIRED`。创建为 201，替换为 204；LOCK 已创建的空文件随后 PUT 为 204。 |
| MKCOL | 只创建一层空目录，不自动创建父目录；已有路径返回 405，非空请求体返回 415。 |
| MOVE | 同一 export 内重命名文件或目录；默认 Overwrite `T`，仅支持文件覆盖文件。目标不存在返回 201，覆盖返回 204；`F` 的目标已存在返回 412。目录覆盖明确拒绝。源目标都执行路径授权、锁和元数据边界检查；跨元数据 scope 返回 `METADATA_SCOPE_CROSS_MOVE`。 |
| DELETE | 文件或空目录删除。非空目录返回 `409 ENOTEMPTY`，首版明确不递归删除；Depth 只能省略或 infinity。根路径不可删除。 |
| LOCK | 独占 write lock。文件支持 Depth `0`/infinity，目录只支持 infinity 子树锁；目录 Depth `0` 明确拒绝。不支持 shared lock。未映射 URL 原子创建零字节文件并持有租约；失败可能保留已发布文件时明确报告 partial。 |
| UNLOCK | 需要匹配 URL、主体和 Lock-Token。成功为 204；未知/已过期令牌返回 `409 LOCK_TOKEN_UNKNOWN`。 |
| COPY/PROPPATCH/其他 | 返回 `501 DAV_METHOD_UNSUPPORTED`；不修改文件、属性或权限。原生 metadata/Seq API 保留自身语义并遵守共享锁。 |

属性包括 displayname、resourcetype、getcontentlength、getcontenttype、getlastmodified、creationdate、getetag、supportedlock、lockdiscovery。未知属性在 207 的 propstat 中返回 404；只读 export 没有可用版本时 getetag 返回 404。锁发现只向持有主体返回其活动令牌，令牌不进入日志。XML 请求上限 64 KiB，禁用 DTD；名称无法用 XML 表示时返回明确错误。URI 只解码一次，拒绝编码分隔符、路径穿越、符号链接及保留目录。

条件语法为单个强 ETag 的 If-Match 或 If-None-Match `*`；不能同时提供。DAV If 支持单个正向锁令牌列表 `(<opaquelocktoken:...>)`，也支持以同源 URI 标记源或目标的列表；多个带标签的列表均须满足，并核对令牌所属资源。Not、OR、多令牌单列表和 If 内的 ETag 条件未实现，返回 `400 DAV_IF_UNSUPPORTED`，绝不当作无条件请求。读方法不接受 DAV If，使用普通 HTTP 条件头。MOVE 的 Destination 可为绝对路径或同源 URI；跨 origin 拒绝，跨 export 返回 `502 DAV_CROSS_EXPORT_MOVE`，不做复制后删除。

## 共用约束、保存与锁生命周期

两个协议共用 `writing::Replacement` 的流式暂存、上传 journal、取消/期限、原子发布、权限保留及返回的版本。版本只在真正的提交临界区检查。客户端明确提供的 ETag 不会被写前 stat 替换；锁写入是独立的“当前独占租约持有者”条件，不声称验证客户端在加锁前读过的缓存版本。锁只保护其持有期间，客户端必须在取得锁后刷新旧缓存。

`core::file_constraints` 在原生上传发布、结构及 metadata 修改、Seq 事务及资源目录创建的实际临界区检查锁；基于目录 inode 和父目录 inode/名称识别资源，同一底层目录的别名和 bind mount 不以别名分裂锁。覆盖、目录子树、MOVE 的源和目标都会纳入检查。PUT 文件原子替换保持目标 URL 租约；MOVE/DELETE 释放源上及其后代的租约，不将源 URL 租约转移到目标，目标和祖先已有租约保留。客户端应在新 URL 重新加锁。

首版采用保守的进程级执行屏障：只要存在活动 DAV 租约，所有 Cells 命令、原生 harness activity 和拓扑修改的准入都拒绝；已有执行尚未物理收尾时，新锁和文件提交拒绝。读请求继续可用。启用 WebDAV 后各文件提交短时串行化，防止加锁、别名操作与执行准入竞态；不提供按无关目录细分的执行并行度。没有活动租约且未发生内容提交时，执行准入仍沿用原生命周期。若进程清理/所有权未知导致 poison，写入及新锁以 `503 FILE_EXECUTION_UNKNOWN` fail closed，不能通过另一个入口继续写；需先核对原执行与持久回执，再由宿主正式恢复。直接在 Cells 管理之外启动的宿主进程不受此进程内屏障保护，不应同时修改这些导出目录。

超时默认 600 秒，`Second-N` 最多 3600 秒；Infinite 按默认有限值处理。空 body 的 LOCK 使用原 If 令牌刷新；UNLOCK 主动释放。到期后不再授权写入，惰性回收记录 `LOCK_EXPIRED`。客户端需要在到期前刷新，不能把重新获取的租约当作离线旧版本的许可。租约不跨服务重启恢复：旧令牌的条件写返回 412，旧 UNLOCK 返回 409；重启后的原生版本 nonce 也变化。重新挂载/读取当前内容、比较本地副本后再加锁，不自动续用旧令牌。

编辑器临时文件替换保存可创建临时文件、上传后 MOVE 覆盖同一 export 的普通目标文件；目标已锁时必须提交目标令牌，临时文件和目标都锁定时须分别提交相应 URI 的令牌。数据与元数据使用现有维护链路；无法保证的目录覆盖、递归删除和跨导出移动直接拒绝，不模拟为多次非原子写入。只支持跨元数据边界之外的同范围 MOVE，不能把一次编辑器保存推广为跨多个请求的事务。

## 失败、断线与恢复

所有请求回送 `X-Operation-Id`；客户端未给时由服务端生成。PUT/MKCOL/MOVE/DELETE/LOCK/UNLOCK 注册现有 Operations 账本，主体、alias 和 ID 共同限定结果查询：`GET /v1/fs/<alias>/operations/<id>`，使用原主体凭据。ID 重用返回 `409 OPERATION_ID_REUSED`；通用客户端重新发送时若没有原 ID，会生成另一个操作，不能视作天然幂等。

写工作在传递追踪的后台任务中持有生命周期和清理责任，HTTP 等待者断线不会自动重放；收到完整请求体之后仍可能完成提交。上传流截断记录 `UPLOAD_DISCONNECTED`，已暂存数据由既有 journal/Drop 清理，异常退出由原资源恢复机制处理。期限到达时返回 `504` 和 `outcome: unknown`，之后仍须查询原操作。工作线程在提交阶段丢失时以 `FILE_COMMIT_WORKER_LOST` / `FILE_LOCK_WORKER_LOST` 报告 unknown，不猜测未提交。账本仅当前进程内保留，已结束操作从创建时起保留最多一小时；重启/过期后 404 不是“未提交”的证据。

| 结果 | 操作步骤 |
| --- | --- |
| 401 / 403 | 核对主体、凭据、export/principal 写权限和项目声明；不因改用 DAV 绕过只读。 |
| 412 | 保存本地副本，重新 GET 当前内容及 ETag，核对锁是否超时/重启/版本变化，人工合并后以新条件提交。 |
| 423 / `FILE_LOCK_CONFLICT` | 保留本地数据，查询自身 lockdiscovery，等待持有者释放或租约到期；不得关闭 use_locks 强行重试。 |
| 409 / `EBUSY` | 已有执行仍持有资源；先查看原执行/清理回执，收尾后重新读取再操作。 |
| 428 | 客户端没有提供写入版本或有效锁；修正客户端锁设置，不把服务端当前 stat 当作已读版本。 |
| 413 / 507 / 上传错误 | 上传超限或空间不足等明确失败；保留本地文件，处理原因并核对原结果。 |
| `partial` | 命名空间已修改，随后同步或元数据维护失败，例如 `FILE_PUBLISHED_SYNC_FAILED`、`FILE_DIRECTORY_SYNC_FAILED`、`FILE_METADATA_MAINTENANCE_FAILED`。先检查实际源/目标、版本和元数据，不能重放 MOVE/DELETE 或宣称回滚。 |
| 断线 / `unknown` / 重启后账本未知 | 按原操作身份查询并关联审计；若记录已不可用，手工核对源、目标、内容和版本。对比前保留本地缓存及恢复副本，禁止自动重放未知副作用。 |

davfs2 的缓存可能仍持有未上传数据。冲突/网络恢复时先将待保存版本复制到挂载点之外；检查客户端的缓存及 lost+found（实际位置取决于挂载用户和客户端配置），保留恢复文件，然后正常卸载并等到卸载完成。不要在仍挂载时删除缓存，不把强制/惰性卸载等同于成功上传。确认远端实际结果后重新挂载，人工合并，再提交。

诊断事件包括 `webdav.finished`、`webdav.commit.finished`、`webdav.lock.finished`、`file.lock.expired`、`file.upload.stage`、`file.upload.cleanup` 及执行准入拒绝；包含操作、阶段、结果、具体错误分类、耗时、operationId 和继承的请求追踪。协议收到 HTTP 状态不代替业务 outcome；日志不包含 XML/文件正文、完整参数、令牌或凭据。相关实现位于 `src/plugin/files/webdav/`、`writing.rs`、`fs/{upload,mutation,constraints,seq}.rs` 和 `src/core/{file_constraints,gate}.rs`。协议参考 [RFC 4918](https://www.rfc-editor.org/rfc/rfc4918)。
