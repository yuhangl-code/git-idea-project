/**
* 容错机制
*  1、检查点（checkpoint）：将之前某个时间点所有的状态保存下来
*      1.1 备份和保存的是状态
*      1.2 任务在运行过程中遇到故障，可以从检查点中恢复到之前的状态
*      1.3 故障恢复之后继续处理的结果，应该与发生故障前完全一致，保证检查结果的一致性
*
*  2、检查点的保存
*      2.1 周期性的触发保存：比如每十分钟触发保存一份状态，间隔时间可以进行设置
*      2.2 保存的时间点：所有的任务都恰好处理完一个相同的输入数据的时候，再恢复数据时可以完整的恢复一个数据链
*                     需要源任务把偏移量作为算子保存下来，而且外部数据存储系统能够重置偏移量
*      2.3 保存的具体流程：以WordCount为例
*
*  3、从检查点恢复状态
*      3.1 当发生故障时，需找到最近一次成功保存的检查点来恢复状态
*          具体步骤：重启应用，重启后所有任务的状态会被清空
*                  读取检查点，重置状态；读取每个算子任务状态的快照，分别填充到对应的状态中
*      3.2 重置偏移量（与就是说：处理一半的数据未被恢复）
*          为了不丢数据，冲保存检查点后开始重新读取数据，可以通过Source任务外部数据源重新提交偏移量
*          ==》在分布式系统中，实现了"精准一次"（exactly-once）
*
*  4、检查点算法
*      4.1 采用了Chandy-Lamport算法的分布式快照，可以在不暂停整体流处理的前提下，将状态备份保存到检查点
*      4.2 检查点分界线（Barrier）
*           收到保存检查点的指令后，Source任务可以在当前数据流中插入这个结构，之后所有的任务遇到它就开始对状态做持久化快照保存
*           因此遇到这个标识就代表之前的数据都处理完了，可以保存一个检查点；之后遇到的数据需保存至下一个检查点
*      4.3 JobManager 有一个 检查点协调器 ：专门用来协调处理检查点的相关工作
*              会定期向TaskManager发出指令，要求保存检查点（带着检查点ID）
*              TM会让所有的source任务把自己的偏移量保存起来，并将带有检查点ID的分界线插入到当前的数据流中，然后像正常数据一样向下游传递
*
*  5、分布式快照算法（Barrier对齐的精准一次）
*      5.1 barrier：之前所有数据的状态更改保存入当前检查点
*      5.2 使用Chandy-Lamport算法的一种变体：（异步分界线快照算法）
*          上游任务向多个并行下游任务发送barrier时，需要广播出去；
*          多个上游任务向同一个下游子任务传递分界线时，会执行一个分界线对齐，也就是需要等到所有并行分区的barrier都到齐，才开始状态的保存
*
*  6、分布式快照算法（Barrier对齐的至少一次）
*      6.1 与精准一次的区别
*          在barrier之前等待的数据，直接计算等到下一个Barrier到达时做状态的保存
*          重新启动时介于两个barrier之间分界线已经到达的分区任务传过来的数据会被再次计算
*
*  7、分布式快照算法（非Barrier对齐的精准一次）
*      7.1 核心思想：只要in-flight的数据也存到状态里，barrier就可以越过所有in-flight的数据继续往下游传递
*      7.2 直接将barrier放到输出缓冲区末端，向下游传递
*          标记数据（标记越过输入缓冲区和输出缓冲区的数据；表数据其它barrier之前的所有数据）
*          把标记数据和状态一起保存到checkpoint中，从checkpoint恢复时这些数据也会一起恢复到对应的位置
*
*  8、总结：
*      8.1 barrier对齐：一个Task 收到 所有上游 同一个编号的barrier之后，才会对自己的本地状态做 备份
*              精准一次：在对齐过程中，barrier后面的数据阻塞等待，不会越过barrier
*              至少一次：在对齐过程中，先到的barrier，其后面的数据不阻塞，接着计算
*
*      8.2 非barrier对齐：一个Task 收到 第一个barrier时，就开始 执行备份；能保证 精准一次（flink 1.11出的新算法）
*              先到的barrier，将本地 状态 做备份，其后面的数据接着计算输出
*              未到的barrier，其 前面的数据 接着计算输出，同时 也保存到 备份中
*              最后一个barrier到达 该Task时，这个Task的备份结束
*
*/

// 每个算子的一个并行度实例就是一个 subTask。由于 Flink 的 TaskManager 运行 Task 的时候是每个 Task 采用一个单独的线程，
// 这会带来很多线程切换和数据交换的开销，进而影响吞吐量。为了避免数据在网络或线程之间传输导致的开销，Flink 会在 JobGraph 阶段，
// 将代码中可以优化的算子优化成一个算子链（Operator Chains）以放到一个 Task 中执行。
this.chainingEnabled = params.get("chainingEnabled", "enabled");
// 每个算子的并行度。并行度设置优先级是：算子设置并行度 > env 设置并行度 > 配置文件默认并行度
this.parallelism = params.getInt("parallelism", 2);
// 状态后端的路径
this.statePath = params.get("statePath", "hdfs://nameservice1/user/shulan/flink/states/" + pathName);
// Flink检查点本质上是通过异步屏障快照（asychronous barrier snapshot, ABS）算法产生的全局状态快照，一般是存储在分布式文件系统（如HDFS）上。
// 但是，如果状态空间超大（比如key非常多或者窗口区间很长），检查点数据可能会达到GB甚至TB级别，每次做checkpoint都会非常耗时。
// 但是，海量状态数据在检查点之间的变化往往没有那么明显，增量检查点可以很好地解决这个问题。顾名思义，
// 增量检查点只包含本次checkpoint与上次checkpoint状态之间的差异，而不是所有状态，变得更加轻量级了。
this.enableIncrementalCheckPointing = params.getBoolean("enableIncrementalCheckPointing", true);
//缓冲区超时时间：为了控制吞吐量和执行时间，你可以在执行环境（或独立的Operator）中调用env.setBufferTimeout(timeoutMillis)来设
// 置等待装满buffer的最大等待时间，在这个时间过后，不管buffer是否已满，它都会自动发出。该默认超时时间是100ms。
this.bufferTimeout = params.getLong("bufferTimeout", 10);
// 开启CheckPoint
this.enableCheckPointing = params.get("enableCheckPointing", "enabled");
// 指定checkpoint的触发间隔(单位milliseconds)
this.checkpointInterval = params.getLong("checkpointInterval", 20 * 60 * 1000);
// 指定checkpoint执行的超时时间(单位milliseconds)，超时没完成就会被终止掉
this.checkpointTimeout = params.getLong("checkpointTimeout", 10 * 60 * 1000);
// 指定在checkpoint发生异常的时候，是否应该fail该task，默认为true，如果设置为false，则task会拒绝checkpoint然后继续运行
this.failOnCheckPointingErrors = params.getBoolean("failOnCheckPointingErrors", false);
// 用于指定运行中的checkpoint最多可以有多少个，用于包装topology不会花太多的时间在checkpoints上面
this.maxConcurrentCheckpoints = params.getInt("maxConcurrentCheckpoints", 1);
// 用于指定checkpoint coordinator上一个checkpoint完成之后最小等多久可以出发另一个checkpoint，
// 当指定这个参数时，maxConcurrentCheckpoints的值为1
this.minPauseBetweenCheckpoints = params.getLong("minPauseBetweenCheckpoints", 10 * 60 * 1000);
/**
* 重启策略【failureRate、failureInterval、delayInterval】 ：默认1天 允许任务失败10次，任务失败后过5秒 再重启
*/
// 失败率：每个测量时间间隔最大失败次数 10次
this.failureRate = params.getInt("failureRate", 10);
// 测量的时间间隔 1
this.failureInterval = params.getInt("failureInterval", 1);
// 两次连续重启尝试的时间间隔 5秒
this.delayInterval = params.getInt("delayInterval", 5);
// 状态后端类型：rocksdb
this.stateBackend = params.get("stateBackend", "rocksdb");