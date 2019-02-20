# 优先 Leader  
## 目标
优先选举某个节点为 Leader，以控制负载。
假设某个组中包含 A B C 三个节点，指定 A 为优先节点，则期望以下自动行为：
1. 如果 A 节点在线，且与其它节点的日志高度差别不大时，优先选举 A 为 Leader
2. 如果 A 节点掉线后又上线，且日志高度追到与当前 Leader 较为接近时，当前 Leader 节点要尝试让出 Leader 角色给 A 节点

## 方案

优先Leader的整体方案依赖抢主（Take Leadership）和主转让(Leadership Transfer)两个子操作。

### 抢主 Take Leadership
Take Leadership 是指当前节点在想要主动成为Leader角色时，执行的抢主操作。
1. 如果当前节点是Follower, 将term+1，转为Candidate，进入抢主状态。如果已经是Candidate，也将term+1，进入抢主状态，具备如下特性：
2. 在日志高度一样时，不会为同一个term的其他Candidate投票
3. 将会以更小的重试间隔（voteInterval）进行选主。
4. 状态只维持一个term的选举。
5. 其他操作同普通的选主过程

抢主操作并不保证改节点一定能成为主。

### 主转让 Leadership Transfer

指定节点转让 Leader 角色是优先 Leader 的一个依赖功能，同时也可以作为独立的功能存在，实现手动的负载均衡。下面是描述其实现方案：

1. 首先 Leader 节点（假设为A节点）提供接口，接收将当前的Leadership 转让给 B 节点。
2. A 节点收到转让命令之后，为了避免过长时间的不可用，先检查B节点的落后的数据量 是否小于 配置的阈值（maxLeadershipTransferWaitIndex)，超了则返回转换失败，结束任务。
3. 如果B节点进度落后不多，则标记 A 节点状态为 "切换"，拒绝接收新的数据写入。
4. A 节点检查B节点的日志数据是否已经是最新，如果还缺数据，则 A 节点继续往 B 节点推送数据。
5. 当 B 节点的数据同步完成之后，A 节点往 B 节点发送命令，让其进行 Take Leadership 操作，争抢主节点。


### Preferred Leader
在 Preferred 节点上，选举的过程类似“抢主”的逻辑，但是不提升Term，避免打断集群。只有收到明确的“抢主”命令时才提升Term。
在 Leader 上，轮询检查，优先节点是否为主，如果不是主并且该节点健康，就发起 Leadership Transfer 操作, 尝试将主节点转让给该优先节点。
