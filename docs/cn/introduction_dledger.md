# DLedger——基于 Raft 的 Commitlog 存储 Library


## 故事的起源
自分布式系统诞生以来，容灾和一致性，一直是经常被讨论的话题。  
Master-Slave 架构是最容易被想到的设计，简单而易于实现，被早期大部分分布式系统采用，包括RocketMQ早期的高可用架构，其略显粗陋的一致性保证、缺少自动 Failover 等，并不能满足需求。  
后来，Hadoop 迅猛发展改变了这一面貌。Hadoop 生态里面的 Zookeeper 组件，可以作为一个高可用的锁而存在，由此引发了大量系统通过 Zookeeper 选主，然后主备复制日志，来达到高可用和一致性的目的。Hadoop 自身 [NameNode 组件](https://hadoop.apache.org/docs/r2.9.2/hadoop-project-dist/hadoop-hdfs/HDFSHighAvailabilityWithQJM.html) 的高可用机制便是这一典型实现。  

基于 ZooKeeper 的设计，通过一些复杂的写入 fence，基本可以满足需求。但 Zookeeper 自身的复杂性，加重了整个设计，在具体实施和运维时，不仅增加资源成本，还累积了系统风险，让维护人员叫苦不堪。

再后来，Raft 论文的出现，再一次改变了局面。其简洁易懂的设计，没有任何外部依赖，就可以轻松搞定一个高可靠、高可用、强一致的数据复制系统，让广大分布式系统研发人员如获至宝。

本文的主角 DLedger 就是这样的一个实践者。

## DLedger 的定位
DLedger 定位是一个工业级的 Java Library，可以友好地嵌入各类 Java 系统中，满足其高可用、高可靠、强一致的需求。  
和这一定位比较接近的是 [Ratis](https://github.com/apache/incubator-ratis)。  
Ratis 是一个典型的"日志 + 状态机"的实现，虽然其状态机可以自定义，却仍然不满足消息领域的需求。
在消息领域，如果根据日志再去构建“消息状态机”，就会产生 Double IO 的问题，造成极大的资源浪费，因此，在消息领域，是不需要状态机的，日志和消息应该是合二为一。  
相比于 Ratis，DLedger 只提供日志的实现，只拥有日志写入和读出的接口，且对顺序读出和随机读出做了优化，充分适应消息系统消峰填谷的需求。   
DLedger 的纯粹日志写入和读出，使其精简而健壮，总代码不超过4000行，测试覆盖率高达70%。而且这种原子化的设计，使其不仅可以充分适应消息系统，也可以基于这些日志去构建自己的状态机，从而适应更广泛的场景。  
综上所述，DLedger 是一个基于 Raft 实现的、高可靠、高可用、强一致的 Commitlog 存储 Library。

## DLedger 的实现
DLedger 的实现大体可以分为以下两个部分：
1.选举 Leader
2.复制日志
其整体架构如下图
![DLedger Architect](https://img.alicdn.com/5476e8b07b923/TB1bwJOycfpK1RjSZFOXXa6nFXa)

本文不展开讨论实现的细节，详情可以参考论文[1]。有兴趣的也可以直接参看源码，项目总体不超过4000行代码，简洁易读。

## DLedger 的应用案例
在 Apache RocketMQ 中，DLedger 不仅被直接用来当做消息存储，也被用来实现一个嵌入式的 KV 系统，以存储元数据信息。

更多的接入案例，敬请期待。

#### 案例1 DLedger 作为 RocketMQ 的消息存储
架构如下图所示：
![DLeger Commitlog](https://img.alicdn.com/5476e8b07b923/TB1RaBNyirpK1RjSZFhXXXSdXXa)
其中：
1. DLedgerCommitlog 用来代替现有的 Commitlog 存储实际消息内容，它通过包装一个 DLedgerServer 来实现复制；
2. 依靠 DLedger 的直接存取日志的特点，消费消息时，直接从 DLedger 读取日志内容作为消息返回给客户端；
3. 依靠 DLedger 的 Raft 选举功能，通过 RoleChangeHandler 把角色变更透传给 RocketMQ 的Broker，从而达到主备自动切换的目标

#### 案例2 利用 DLedger 实现一个高可用的嵌入式 KV 存储
架构图如下所示：
![DLedger KV](https://img.alicdn.com/5476e8b07b923/TB1aCpYygHqK1RjSZFEXXcGMXXa)

其中：
1. DLedger 用来存储 KV 的增删改日志；
2. 通过将日志一条条 Apply 到本地 Map，比如 HashMap 或者 第三方 的 RocksDB等

整个系统的高可用、高可靠、强一致通过 DLedger 来实现。

## 社区发展计划

目前 DLedger 已经成为 [OpenMessaging](https://github.com/openmessaging) 中存储标准的默认实现。DLedger 会维持自身定位不变，作为一个精简的 Commitlog 存储 Library，后续主要是做性能优化和一些必要的特性补充。  
基于 DLedger 的开发，也可以作为独立项目进行孵化，比如 [OpenMessaging KV](https://github.com/openmessaging/openmessaging-hakv)。  
欢迎社区的朋友们一起来共建。  

## References
[[1] Diego Ongaro and John Ousterhout. 2014. In search of an understandable consensus algorithm. In Proceedings of the 2014 USENIX conference on USENIX Annual Technical Conference (USENIX ATC'14), Garth Gibson and Nickolai Zeldovich (Eds.). USENIX Association, Berkeley, CA, USA, 305-320](https://www.usenix.org/system/files/conference/atc14/atc14-paper-ongaro.pdf)

