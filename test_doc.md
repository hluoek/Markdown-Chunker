# 分布式任务调度系统技术文档

## 1. 系统概述

本文档描述了一套面向大规模数据处理场景的分布式任务调度系统（DistTaskScheduler，下称 DTS）的设计规范与实现细节。DTS 旨在解决传统单机调度框架在高并发、大数据量场景下的性能瓶颈，提供毫秒级任务分发、自动故障转移、动态资源弹性伸缩等核心能力。

系统整体基于 Master-Worker 架构设计。Master 节点负责全局任务队列管理、Worker 节点心跳维护、资源水位感知与调度决策；Worker 节点负责本地任务执行、状态上报与结果回写。Master 与 Worker 之间通过 gRPC 长连接通信，所有元数据持久化至 etcd 集群，任务结果写入 Redis Cluster，最终归档至 HBase。

DTS 的设计目标包括：单集群支持 10 万级并发任务、P99 调度延迟低于 50ms、节点故障恢复时间低于 5 秒、支持有向无环图（DAG）类型的复杂依赖任务编排、支持优先级抢占与公平调度双模式、支持插件化资源驱动（Kubernetes、YARN、裸金属）。

本文档面向系统开发者、运维工程师和高级用户，假设读者具备分布式系统基础知识、Python 3.10+ 开发经验以及基本的 Linux 操作能力。



## 2. 架构设计

### 2.1 整体架构

DTS 由以下核心组件构成：Master 调度器、Worker 执行器、TaskRouter 路由层、MetaStore 元数据层、ResultStore 结果层、MonitorAgent 监控代理。各组件间的通信协议、数据格式与部署拓扑详见后续章节。

整个系统的请求链路如下：客户端通过 REST API 或 Python SDK 提交任务，TaskRouter 对任务进行合法性校验、优先级打标与限流控制，通过后写入 Master 的全局就绪队列。Master 调度线程持续扫描就绪队列，依据当前 Worker 资源水位与任务亲和性规则选择目标 Worker，通过 gRPC 下发任务。Worker 完成执行后将结果写入 ResultStore，并通过心跳或主动回调通知 Master 更新任务状态。

### 2.2 Master 节点设计

Master 是系统的调度核心，采用主备高可用部署，通过 etcd 分布式锁实现 Leader 选举。主 Master 持有全局调度权，备 Master 实时同步元数据，主节点故障时备节点在 3 秒内完成接管。

Master 内部分为五个核心模块：QueueManager（队列管理）、SchedulerEngine（调度引擎）、WorkerRegistry（Worker 注册中心）、HeartbeatMonitor（心跳监控）、DAGResolver（DAG 依赖解析）。各模块通过内部事件总线解耦，支持独立扩展。

### 2.3 Worker 节点设计

Worker 节点支持异构部署，可运行于物理机、虚拟机或容器环境。每个 Worker 在启动时向 Master 注册，上报自身的 CPU 核数、内存容量、GPU 数量（如有）、网络带宽、标签集合（用于亲和性调度）等资源信息。

Worker 内部维护本地执行队列，支持并发执行多个任务槽（slot），每个 slot 独立管理任务生命周期。Worker 通过 cgroup 对每个任务进行资源隔离，防止单任务异常影响其他任务。

#### 2.3.1 Worker 资源模型

Worker 资源模型采用静态声明 + 动态感知相结合的方式。静态声明在注册时提交，动态感知通过 MonitorAgent 每 10 秒采集一次实时资源使用情况并上报 Master，Master 据此动态调整该 Worker 的可用资源视图。

#### 2.3.2 Worker 故障处理

Worker 故障分为两类：软故障（心跳超时但进程存活）和硬故障（进程崩溃或网络隔离）。软故障由 HeartbeatMonitor 在 15 秒内检测并触发任务重新调度；硬故障由 etcd session 过期机制在 30 秒内触发，Master 将该 Worker 上所有运行中的任务标记为失败并重新入队。



## 3. 核心数据结构

下表列出了任务提交时支持的全部字段，包含字段名称、数据类型、是否必填、默认值及详细说明。调度系统依据这些字段进行资源匹配、优先级排序与依赖解析。所有时间字段均使用 UTC 时间戳（秒级），所有资源字段均使用国际单位制。

| 字段名 | 数据类型 | 是否必填 | 默认值 | 取值范围 | 说明 |
|---|---|---|---|---|---|
| task_id | string | 是 | 无 | UUID v4 格式 | 全局唯一任务标识符，由客户端生成并保证唯一性，重复提交同一 task_id 将被幂等处理 |
| task_name | string | 否 | "" | 长度 1-256 | 人类可读的任务名称，仅用于展示，不参与调度逻辑 |
| task_type | enum | 是 | 无 | SHELL / PYTHON / SPARK / FLINK / CUSTOM | 任务类型，决定 Worker 的执行器选择逻辑，CUSTOM 类型需配合 executor_image 字段使用 |
| priority | integer | 否 | 50 | 1-100 | 任务优先级，数值越大优先级越高，100 为最高优先级，相同优先级按提交时间 FIFO 排序 |
| max_retry | integer | 否 | 3 | 0-10 | 最大重试次数，0 表示不重试，每次重试间隔按指数退避策略计算，基础间隔 5 秒 |
| timeout_seconds | integer | 否 | 3600 | 10-86400 | 任务最大执行时间（秒），超时后 Worker 强制终止任务并标记为 TIMEOUT 状态 |
| cpu_request | float | 否 | 1.0 | 0.1-256.0 | 任务所需 CPU 核数（逻辑核），调度时 Worker 可用 CPU 必须满足此值 |
| memory_request_mb | integer | 否 | 512 | 128-1048576 | 任务所需内存量（MB），调度时 Worker 可用内存必须满足此值 |
| gpu_request | integer | 否 | 0 | 0-8 | 任务所需 GPU 数量，0 表示无需 GPU，大于 0 时仅调度至具有对应数量空闲 GPU 的 Worker |
| node_selector | dict | 否 | {} | key-value 标签对 | Worker 节点亲和性标签选择器，所有标签必须全部匹配，空字典表示无亲和性要求 |
| dependencies | list[string] | 否 | [] | task_id 列表 | 前置依赖任务 ID 列表，列表中所有任务成功完成后本任务才会进入就绪状态 |
| env_vars | dict | 否 | {} | key-value 字符串对 | 注入任务执行环境的环境变量，会覆盖 Worker 默认环境变量中同名项 |
| executor_image | string | 条件必填 | 无 | 合法镜像地址 | task_type 为 CUSTOM 时必填，指定任务执行使用的容器镜像地址 |
| result_ttl_seconds | integer | 否 | 86400 | 60-2592000 | 任务结果在 ResultStore 中的保留时长（秒），超期自动清理 |
| tags | list[string] | 否 | [] | 字符串列表，每项长度 1-64 | 任务标签，用于任务查询过滤，不参与调度逻辑 |
| created_at | integer | 自动填充 | 服务器时间 | UTC 时间戳 | 任务创建时间，由服务端在接收到任务时自动填充，客户端提交的值会被覆盖 |
| started_at | integer | 自动填充 | 无 | UTC 时间戳 | 任务开始执行时间，由 Worker 在任务启动时上报 |
| finished_at | integer | 自动填充 | 无 | UTC 时间戳 | 任务结束时间（无论成功、失败或超时），由 Worker 在任务结束时上报 |
| status | enum | 自动填充 | PENDING | PENDING / READY / RUNNING / SUCCESS / FAILED / TIMEOUT / CANCELLED | 任务状态机当前状态，仅由系统内部更新，客户端不可直接设置 |
| worker_id | string | 自动填充 | 无 | Worker UUID | 实际执行该任务的 Worker 节点 ID，调度后由 Master 填充 |
| exit_code | integer | 自动填充 | 无 | 0-255 | 任务进程退出码，0 表示正常退出，非 0 表示异常，TIMEOUT 状态下为 -1 |
| error_message | string | 自动填充 | "" | 长度 0-4096 | 任务失败时的错误摘要信息，由 Worker 采集并上报，超长部分截断 |


## 4. 核心模块实现

### 4.1 调度引擎

调度引擎是 DTS 的核心组件，负责从就绪队列中取出任务并匹配合适的 Worker 节点。调度算法综合考虑资源匹配度、节点亲和性、负载均衡和优先级四个维度，最终输出一个（任务, Worker）匹配对列表，交由通信层执行下发。调度引擎设计为无状态，所有状态均从 WorkerRegistry 和 QueueManager 实时读取，便于后续水平扩展。

以下是调度引擎与 Worker 注册中心的完整实现，包含资源匹配、得分计算、心跳管理与故障检测等核心逻辑：

```python
class WorkerInfo:
    worker_id: str
    hostname: str
    total_cpu: float
    total_memory_mb: int
    total_gpu: int
    available_cpu: float
    available_memory_mb: int
    available_gpu: int
    labels: dict = field(default_factory=dict)
    last_heartbeat: float = field(default_factory=time.time)
    running_tasks: list = field(default_factory=list)
    is_alive: bool = True

    def can_fit(self, task: Task) -> bool:
        """判断当前 Worker 是否能容纳指定任务的资源需求。"""
        if self.available_cpu < task.cpu_request:
            return False
        if self.available_memory_mb < task.memory_request_mb:
            return False
        if self.available_gpu < task.gpu_request:
            return False
        for key, value in task.node_selector.items():
            if self.labels.get(key) != value:
                return False
        return True

    def allocate(self, task: Task) -> None:
        """为任务预留资源并记录运行中任务列表。"""
        self.available_cpu -= task.cpu_request
        self.available_memory_mb -= task.memory_request_mb
        self.available_gpu -= task.gpu_request
        self.running_tasks.append(task.task_id)

    def release(self, task: Task) -> None:
        """任务结束后归还资源。"""
        self.available_cpu = min(self.total_cpu,
                                 self.available_cpu + task.cpu_request)
        self.available_memory_mb = min(self.total_memory_mb,
                                       self.available_memory_mb + task.memory_request_mb)
        self.available_gpu = min(self.total_gpu,
                                 self.available_gpu + task.gpu_request)
        if task.task_id in self.running_tasks:
            self.running_tasks.remove(task.task_id)

    def utilization_score(self) -> float:
        """
        计算 Worker 负载得分，用于调度时优先选择负载较低的节点。
        得分越低表示节点越空闲，调度优先级越高。
        综合 CPU、内存、GPU 三个维度的已用比例加权平均：
        CPU 权重 0.5，内存权重 0.3，GPU 权重 0.2。
        """
        cpu_used_ratio = 1.0 - (self.available_cpu / self.total_cpu) \
            if self.total_cpu > 0 else 1.0
        mem_used_ratio = 1.0 - (self.available_memory_mb / self.total_memory_mb) \
            if self.total_memory_mb > 0 else 1.0
        gpu_used_ratio = 1.0 - (self.available_gpu / self.total_gpu) \
            if self.total_gpu > 0 else 0.0
        return cpu_used_ratio * 0.5 + mem_used_ratio * 0.3 + gpu_used_ratio * 0.2

    def heartbeat_age_seconds(self) -> float:
        return time.time() - self.last_heartbeat

    def refresh_heartbeat(self) -> None:
        self.last_heartbeat = time.time()
        self.is_alive = True

```

### 4.2 DAG 依赖解析

DAG 解析模块负责在任务提交时检测循环依赖，并在任务完成时触发后继任务进入就绪状态。模块采用拓扑排序算法，支持百万级任务依赖关系的高效解析。所有依赖关系持久化至 etcd，保证 Master 重启后能够完整恢复依赖图状态。

依赖解析的核心流程包括：入度计算、拓扑排序校验（检测环）、就绪队列触发。当一个任务进入 SUCCESS 状态时，DAGResolver 遍历其所有后继任务，对每个后继任务减少其未完成依赖计数，若计数归零则将其推入调度器就绪队列。这一过程为 O(E) 复杂度，其中 E 为依赖边数量。

#### 4.2.1 循环依赖检测

系统在任务提交阶段即进行循环依赖检测，拒绝会形成环的任务提交。检测采用 DFS 染色法（白-灰-黑三色标记），时间复杂度 O(V+E)，在提交延迟上的影响通常低于 5ms（依赖图规模在万级以内时）。

#### 4.2.2 跨批次依赖处理

当依赖任务与被依赖任务不在同一调度批次提交时，系统通过 etcd 的 Watch 机制监听依赖任务的状态变更，保证跨批次依赖的正确触发。所有依赖关系的状态变更均记录审计日志，便于排查依赖死锁问题。


## 5. 部署与运维

### 5.1 环境要求

运行 DTS 需要以下基础环境：Python 3.10 及以上版本、etcd 3.5 及以上版本（集群模式建议 3 节点）、Redis 7.0 及以上版本（建议 Redis Cluster 模式，至少 3 主 3 从）。Master 节点推荐配置为 8 核 16GB 内存，Worker 节点配置依据实际业务负载弹性配置。

网络要求：Master 与 Worker 之间需要 TCP 全双工通信，推荐内网带宽 10Gbps 以上；Master 节点需能访问 etcd 集群的 2379 端口；Worker 节点需能访问 Redis Cluster 的各节点端口（默认 6379-6384）。

### 5.2 安装步骤

首先克隆代码仓库并安装依赖，随后完成配置文件初始化，最后依次启动 etcd、Redis、Master 和 Worker 组件。详细步骤参见部署手册附录 A。生产环境推荐使用 Ansible Playbook 或 Helm Chart 进行自动化部署，避免手动配置引入的人为错误。

### 5.3 监控与告警

DTS 通过 Prometheus Exporter 暴露核心运行指标，建议配置以下告警规则：

| 指标名 | 告警阈值 | 告警级别 | 说明 |
|---|---|---|---|
| dts_schedule_latency_p99_ms | > 200ms 持续 5 分钟 | WARNING | 调度 P99 延迟过高，检查 Worker 资源水位 |
| dts_schedule_latency_p99_ms | > 500ms 持续 2 分钟 | CRITICAL | 调度严重延迟，可能触发任务积压雪崩 |
| dts_worker_alive_count | < 预期数量 * 0.5 | CRITICAL | 存活 Worker 数量低于半数，集群降级 |
| dts_task_failed_rate_1m | > 5% | WARNING | 近 1 分钟任务失败率过高 |
| dts_task_queue_depth | > 10000 | WARNING | 就绪队列积压过深，考虑扩容 Worker |
| dts_etcd_write_latency_ms | > 100ms | WARNING | etcd 写入延迟升高，检查 etcd 集群状态 |


## 6. 常见问题排查

### 6.1 任务长时间停留在 PENDING 状态

当任务长时间无法从 PENDING 进入 READY 或 RUNNING 状态时，首先检查任务的 dependencies 字段，确认所有前置依赖任务均已完成。若依赖均已完成，则检查资源请求是否超过集群任何单一 Worker 的最大可用资源（存在不可调度的超大资源请求）。还需排查 node_selector 配置是否过于严格导致没有符合条件的 Worker。

### 6.2 Worker 频繁报告心跳超时

Worker 心跳超时通常由三类原因引起：网络抖动导致心跳包延迟、Worker 进程因任务执行阻塞了心跳线程（心跳线程与执行线程未隔离）、或 Master 侧处理心跳的线程池打满导致响应延迟。排查时首先查看 Worker 日志中的心跳发送记录，确认心跳确实在按时发出；其次在 Master 侧查看 `dts_heartbeat_process_latency` 指标，判断是否存在处理侧积压。

### 6.3 DAG 任务触发延迟

在依赖任务完成后，后继任务进入就绪队列出现明显延迟时，重点排查 etcd Watch 事件的处理积压。通过 `etcdctl watch --prefix /dts/task/status/` 观察状态变更事件是否及时推送。若 etcd 事件正常但任务未触发，检查 DAGResolver 线程是否存在死锁（通过 `kill -3 <master_pid>` 打印线程栈）。

### 6.4 任务执行输出海量单行异形日志

当分布式任务调度系统在执行涉及海量小文件并发处理、超大规模数据湖元数据同步或高频微服务级联调用的极端高并发场景时，如果业务线开发者在编写执行逻辑的任务脚本中严重缺乏防御性编程意识，错误地将未经任何脱敏和截断处理的深层嵌套结构化数据强制序列化输出，并且在基础设施层面完全没有配置合理的换行符定界、基于时间或大小的日志轮转策略以及背压限流机制，就极有可能导致 Worker 节点上的常驻守护进程在极短的几毫秒时间内，向操作系统的标准输出流狂吐包含大量复杂上下文变量、冗长环境变量、甚至是全链路追踪头信息以及完整网络请求与响应体内存快照的未压缩纯文本流。

这些毫无节制的冗余内容由于从头到尾缺乏天然的换行符或结构化标记进行物理分隔，从而在内存映射的日志缓冲区中形成了一个体量巨大、首尾相连、犹如泥石流一般的超长单行文本巨兽，这不仅会以指数级的速度迅速耗尽宿主机本就非常有限的本地磁盘存储空间与文件系统 inode 节点，直接触发底层操作系统内核级别的磁盘只读保护机制与 cgroup 资源配额熔断，导致该节点上的所有 I/O 操作瞬间夯死、整个系统陷入无法响应任何外部请求的僵死状态，更致命的是，以 DaemonSet 模式部署在物理机节点侧专门负责实时采集、解析和上报业务侧日志的 Fluentd 或 Filebeat 等 Agent 组件，在试图通过系统调用读取到这种动辄数兆甚至数十兆字节且没有任何换行边界的异形脏数据时，其内部用于日志格式匹配和字段提取的正则表达式引擎会不可避免地触发灾难性的回溯陷阱（Catastrophic Backtracking），这种极端的贪婪匹配会在几毫秒内瞬间打满所有的 CPU 逻辑核心，并将有限的堆外内存空间疯狂塞满从而引发进程级别的内存溢出崩溃（OOM Killer 强杀），进而导致该节点上包括基础资源利用率、业务自定义埋点以及调度心跳响应在内的所有监控指标全部在普罗米修斯等监控大盘上丢失，形成可怕的“静默故障”与监控盲区，而此时处于云端的 Master 调度控制面在连续多次未收到该 Worker 节点的健康心跳后，会依据默认的容灾策略将其标记为失联并触发任务重试机制，将这个带有“毒药”属性的致命任务重新分发给集群内其他原本健康的空闲节点执行，从而如法炮制地引发一连串多米诺骨牌式的级联雪崩效应，最终导致整个分布式调度集群的核心算力池全军覆没。针对这种极其恶劣且极具破坏性的生产级重大故障事故，资深运维架构师首先必须分秒必争地跳过常规的控制台操作，立即通过带外管理网络（OOB）或底层的 SSH 强插通道，艰难地登录那些因系统负载极高而几乎无法建立 TCP 握手的故障机器终端，使用最高权限的 kill -9 信号直接从内核态强杀那些已经彻底失控且无法被正常优雅停止的 Worker 进程来强行止血，随后必须立刻通过配置管理中心下发紧急变更指令，将该问题任务的全局日志输出级别从 DEBUG 强制拉高甚至直接重定向至黑洞设备进行丢弃，并熟练利用专业的流式文本处理工具如 sed、awk 或直接使用 truncate 命令对硬盘上残留的动辄数十 GB 的巨大垃圾日志文件进行硬性切割或者原地清空以释放宝贵的存储空间，最后，为了彻底杜绝此类事件的再次发生，技术团队不仅要在日志采集 Agent 的底层配置文件中硬性增加单行日志的最大长度截断阈值，还必须在下游专供研发人员排障诊断使用的基于大语言模型的 RAG 智能运维分析系统中，针对预处理管道引入更为严苛且具备容错能力的基于标点符号、特定系统关键字或深度语义理解的强制分块算法与滑动窗口策略，因为哪怕是最先进的大语言模型，其底层的自注意力机制在面对这种超长且毫无结构断点的上下文时，也会随着序列长度的平方级增长而导致显存溢出或注意力权重彻底涣散，进而使得模型在尝试提取故障根因时被海量的无关噪点撑爆 Token 处理上限，最终产生诸如张冠李戴、胡言乱语等极其严重的分析幻觉，彻底失去智能排障的业务价值。