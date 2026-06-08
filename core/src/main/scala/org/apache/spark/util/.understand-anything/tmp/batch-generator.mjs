#!/usr/bin/env node
/**
 * batch-generator.mjs (v2)
 *
 * Generates batch-N.json for the Spark util analysis with:
 *   - File-level nodes with Chinese summaries/tags (--language zh)
 *   - Class/object sub-nodes (class:type id) for major abstractions
 *   - Inheritance / implementation / depends_on edges
 *   - documents edges from .md to .scala
 */

import { readFileSync, writeFileSync } from 'node:fs';

const PROJECT_ROOT = "F:\\code\\workpace\\study\\intellij idea\\source\\spark\\core\\src\\main\\scala\\org\\apache\\spark\\util";
const SCAN_PATH = `${PROJECT_ROOT}\\.understand-anything\\intermediate\\scan-result.json`;
const BATCHES_PATH = `${PROJECT_ROOT}\\.understand-anything\\intermediate\\batches.json`;
const OUT_DIR = `${PROJECT_ROOT}\\.understand-anything\\intermediate`;

const scan = JSON.parse(readFileSync(SCAN_PATH, 'utf8'));
const batches = JSON.parse(readFileSync(BATCHES_PATH, 'utf8')).batches;

const sizeByPath = new Map();
for (const f of scan.files) sizeByPath.set(f.path, f.sizeLines);

function inferComplexity(n) {
  if (n < 50) return 'simple';
  if (n < 200) return 'moderate';
  return 'complex';
}

// --------------------------------------------------------------------------
// File-level metadata (zh)
// --------------------------------------------------------------------------
const FILE_META = {
  'AccumulatorV2.scala': { summary: 'Spark 通用累加器抽象 AccumulatorV2，定义 add/merge/reset/copy 等契约。', tags: ['累加器', '抽象', '序列化'] },
  'ByteBufferInputStream.scala': { summary: '将 java.nio.ByteBuffer 包装为 java.io.InputStream。', tags: ['io', 'nio', '桥接'] },
  'ByteBufferOutputStream.scala': { summary: '将 java.nio.ByteBuffer 包装为 java.io.OutputStream。', tags: ['io', 'nio', '桥接'] },
  'CausedBy.scala': { summary: '提供从异常链中查找根因的工具对象。', tags: ['异常处理'] },
  'Clock.scala': { summary: '抽象时钟 trait，便于在测试中替换系统时间。', tags: ['时间', '抽象'] },
  'ClosureCleaner.scala': { summary: '清理 Scala 闭包以避免序列化时携带不必要引用，处理 REPL/匿名类等边界情况。', tags: ['闭包', '序列化', '反射'] },
  'CollectionsUtils.scala': { summary: 'Spark 集合相关通用工具。', tags: ['集合', '工具函数'] },
  'CommandLineUtils.scala': { summary: '命令行参数解析工具。', tags: ['命令行', '工具函数'] },
  'CompletionIterator.scala': { summary: '包装迭代器并在消费完毕后触发 onComplete 回调。', tags: ['迭代器', '装饰器'] },
  'DependencyUtils.scala': { summary: 'Spark 外部依赖（Ivy/URL）解析与缓存工具。', tags: ['依赖管理', 'ivy', '工具函数'] },
  'Distribution.scala': { summary: '统计摘要（均值/方差/分位数等）。', tags: ['统计'] },
  'EventLoop.scala': { summary: '基于守护线程的事件循环，AsyncEventQueue 的基类。', tags: ['事件循环', '异步'] },
  'HadoopFSUtils.scala': { summary: '封装 Hadoop FileSystem 常用操作（list/glob/copy 等）。', tags: ['hadoop', '文件系统'] },
  'IdGenerator.scala': { summary: '生成单调递增的 ID。', tags: ['id 生成'] },
  'IntParam.scala': { summary: '将命令行整型参数注册到 SparkConf 的工具。', tags: ['命令行', '配置'] },
  'JsonProtocol.scala': { summary: 'Spark 事件日志的 JSON 序列化/反序列化协议。', tags: ['json', '序列化', '事件日志'] },
  'KeyLock.scala': { summary: '基于键的细粒度锁。', tags: ['并发', '锁'] },
  'ListenerBus.scala': { summary: '事件总线抽象 trait，SparkListenerBus 的基类。', tags: ['事件总线', '观察者'] },
  'ManualClock.scala': { summary: 'Clock 的可手动推进实现，用于测试。', tags: ['时间', '测试替身'] },
  'MemoryParam.scala': { summary: '将内存字符串（如 1g）注册到 SparkConf 的工具。', tags: ['命令行', '配置', '内存'] },
  'MutablePair.scala': { summary: '可变的键值对容器。', tags: ['数据结构', '键值对'] },
  'NextIterator.scala': { summary: '自定义迭代器的样板抽象基类。', tags: ['迭代器', '抽象基类'] },
  'PeriodicCheckpointer.scala': { summary: '在后台线程周期性将 Checkpointable 写入可靠检查点。', tags: ['检查点', '周期任务', '可靠性'] },
  'RpcUtils.scala': { summary: 'Spark RPC 框架的辅助方法。', tags: ['rpc'] },
  'SecurityUtils.scala': { summary: '安全相关工具（认证/授权/SSL 配置读取）。', tags: ['安全', '认证'] },
  'SerializableBuffer.scala': { summary: '可序列化的 NIO ByteBuffer 包装。', tags: ['序列化', 'nio'] },
  'SerializableConfiguration.scala': { summary: '可序列化的 Hadoop Configuration 包装。', tags: ['序列化', 'hadoop'] },
  'SerializableJobConf.scala': { summary: '可序列化的 Hadoop JobConf 包装。', tags: ['序列化', 'hadoop'] },
  'ShutdownHookManager.scala': { summary: '统一管理 JVM 关闭钩子，确保有序清理 Spark 资源。', tags: ['关闭钩子', '资源管理', '生命周期'] },
  'SignalUtils.scala': { summary: 'POSIX 信号处理工具。', tags: ['信号'] },
  'SizeEstimator.scala': { summary: '通过反射估算对象图的堆内存占用。', tags: ['内存估算', '反射'] },
  'SparkExitCode.scala': { summary: 'Spark 进程退出码的统一定义。', tags: ['退出码', '常量'] },
  'SparkFatalException.scala': { summary: '标记 Spark 致命错误的异常类型。', tags: ['异常'] },
  'SparkUncaughtExceptionHandler.scala': { summary: 'Spark 线程未捕获异常的统一处理器。', tags: ['异常处理', '线程'] },
  'StatCounter.scala': { summary: '在线计算均值/方差/最值等统计指标。', tags: ['统计'] },
  'ThreadUtils.scala': { summary: '线程工厂与线程池辅助方法（命名/守护/uncaught）。', tags: ['线程', '线程池'] },
  'UninterruptibleThread.scala': { summary: '对中断免疫的线程，配套 UninterruptibleThreadRunner 保护关键临界区。', tags: ['线程', '中断', '锁'] },
  'UninterruptibleThreadRunner.scala': { summary: '在不可中断线程中运行指定块的执行器。', tags: ['线程', '中断', '锁'] },
  'Utils.scala': { summary: 'Spark 通用工具对象，聚合序列化/IO/时间解析/文件操作/字符串等大量工具方法。', tags: ['工具函数', '核心', 'helpers', 'common'] },
  'VersionUtils.scala': { summary: 'Spark 版本号解析与比较工具。', tags: ['版本'] },
  'collection/AppendOnlyMap.scala': { summary: '只追加的哈希映射，支持聚合合并。', tags: ['数据结构', 'map', 'shuffle', '聚合'] },
  'collection/BitSet.scala': { summary: '基于 Long[] 的可变位图。', tags: ['位图', '数据结构'] },
  'collection/CompactBuffer.scala': { summary: '元素少时使用数组、较多时切换为缓冲的高效可变缓冲。', tags: ['缓冲区', '数据结构'] },
  'collection/ExternalAppendOnlyMap.scala': { summary: '可溢写磁盘的 AppendOnlyMap，groupBy/join 的去耦与聚合。', tags: ['shuffle', 'spill', '聚合', '数据结构'] },
  'collection/ExternalSorter.scala': { summary: '可溢写磁盘的外部排序器，sort-based shuffle 的核心。', tags: ['shuffle', 'spill', '排序', '数据结构'] },
  'collection/ImmutableBitSet.scala': { summary: '不可变位图。', tags: ['位图', '不可变', '数据结构'] },
  'collection/OpenHashMap.scala': { summary: 'Scala mutable Map 的开放寻址高性能替代。', tags: ['map', '哈希', '数据结构'] },
  'collection/OpenHashSet.scala': { summary: 'Scala mutable Set 的开放寻址高性能替代。', tags: ['set', '哈希', '数据结构'] },
  'collection/PairsWriter.scala': { summary: '外部排序/spill 阶段写 (K,V) 对的抽象迭代器。', tags: ['迭代器', 'shuffle', '抽象'] },
  'collection/PartitionedAppendOnlyMap.scala': { summary: '带分区信息的 AppendOnlyMap，用于 partition-by-key 聚合。', tags: ['shuffle', '分区', '聚合'] },
  'collection/PartitionedPairBuffer.scala': { summary: '带分区信息的 (K,V) 缓冲。', tags: ['shuffle', '分区', '缓冲区'] },
  'collection/PercentileHeap.scala': { summary: '近似百分位估算的双堆实现。', tags: ['百分位', '堆', '近似'] },
  'collection/PrimitiveKeyOpenHashMap.scala': { summary: '针对基本类型 key 优化的高性能哈希映射。', tags: ['map', '哈希', '基本类型'] },
  'collection/PrimitiveVector.scala': { summary: '基本类型数组的高效可变缓冲。', tags: ['向量', '基本类型'] },
  'collection/SizeTracker.scala': { summary: '通过抽样估算集合当前大小。', tags: ['大小估算', 'spill'] },
  'collection/SizeTrackingAppendOnlyMap.scala': { summary: '带大小跟踪的 AppendOnlyMap。', tags: ['spill', 'map', '大小估算'] },
  'collection/SizeTrackingVector.scala': { summary: '带大小跟踪的 PrimitiveVector。', tags: ['spill', '向量', '大小估算'] },
  'collection/SortDataFormat.scala': { summary: '排序所需的数据格式抽象（key/comparator）。', tags: ['排序', '抽象', 'shuffle'] },
  'collection/Sorter.scala': { summary: '基于 TimSort 的可溢写排序器。', tags: ['排序', 'timsort', 'shuffle'] },
  'collection/Spillable.scala': { summary: '可溢写集合的基类。', tags: ['spill', '抽象基类'] },
  'collection/Utils.scala': { summary: 'collection 子包的工具函数。', tags: ['工具函数', '集合'] },
  'collection/WritablePartitionedPairCollection.scala': { summary: '可写出分区 (K,V) 的集合抽象。', tags: ['shuffle', '抽象'] },
  'io/ChunkedByteBuffer.scala': { summary: '分块的 NIO ByteBuffer 容器，支持分块序列化与 IO。', tags: ['io', 'nio', '字节缓冲'] },
  'io/ChunkedByteBufferFileRegion.scala': { summary: '将 ChunkedByteBuffer 适配为 Netty FileRegion 的零拷贝传输。', tags: ['io', '零拷贝', 'netty'] },
  'io/ChunkedByteBufferOutputStream.scala': { summary: '可写出到 ChunkedByteBuffer 的 OutputStream。', tags: ['io', '字节缓冲'] },
  'logging/DriverLogger.scala': { summary: 'Driver 端结构化日志管理器，支持重定向到指定文件。', tags: ['日志', 'driver'] },
  'logging/FileAppender.scala': { summary: '基于文件的日志追加器抽象。', tags: ['日志', '抽象'] },
  'logging/RollingFileAppender.scala': { summary: '支持按大小/时间滚动的文件日志追加器。', tags: ['日志', '滚动', '文件'] },
  'logging/RollingPolicy.scala': { summary: '日志滚动策略的抽象与默认实现。', tags: ['日志', '滚动', '策略'] },
  'random/Pseudorandom.scala': { summary: '伪随机数生成器 trait。', tags: ['随机数', '抽象'] },
  'random/RandomSampler.scala': { summary: '采样抽象与多种采样策略（泊松/伯努利/水塘等）。', tags: ['采样', '随机数'] },
  'random/SamplingUtils.scala': { summary: '排序后数据的分层/分位数采样工具。', tags: ['采样'] },
  'random/StratifiedSamplingUtils.scala': { summary: '分层抽样的核心算法与 RDD 辅助函数。', tags: ['采样', '分层'] },
  'random/XORShiftRandom.scala': { summary: '高性能 XORShift 随机数生成器。', tags: ['随机数', '高性能'] },
  'taskListeners.scala': { summary: 'task 监听器相关的辅助定义。', tags: ['task', '监听器', '钩子'] },
  'package.scala': { summary: 'org.apache.spark.util 的 package object，提供包级注释。', tags: ['package', 'barrel'] },
};

// Class/object sub-nodes (only for major abstractions)
const CLASS_NODES = {
  'Utils.scala': [
    { name: 'Utils', kind: 'object', summary: 'Spark 通用工具对象，封装序列化/IO/文件/时间解析/字符串/网络等大量静态工具。', tags: ['工具对象', '单例', 'helpers'] },
  ],
  'Clock.scala': [
    { name: 'Clock', kind: 'trait', summary: '抽象时钟 trait，提供 now()/waitTillTime() 等。', tags: ['时间', 'trait'] },
  ],
  'ManualClock.scala': [
    { name: 'ManualClock', kind: 'class', summary: '可手动推进时间的 Clock 实现，专为测试而设。', tags: ['时间', '测试替身'] },
  ],
  'ListenerBus.scala': [
    { name: 'ListenerBus', kind: 'trait', summary: '事件总线抽象，定义 addListener/postToAll 等契约。', tags: ['事件总线', 'trait'] },
  ],
  'EventLoop.scala': [
    { name: 'EventLoop', kind: 'class', summary: '守护线程驱动的单线程事件循环，AsyncEventQueue 的基类。', tags: ['事件循环'] },
  ],
  'KeyLock.scala': [
    { name: 'KeyLock', kind: 'class', summary: '基于键的细粒度读写锁。', tags: ['并发', '锁'] },
  ],
  'ShutdownHookManager.scala': [
    { name: 'ShutdownHookManager', kind: 'object', summary: 'JVM 关闭钩子统一管理器。', tags: ['关闭钩子', '单例'] },
  ],
  'ThreadUtils.scala': [
    { name: 'ThreadUtils', kind: 'object', summary: '线程工厂与线程池辅助方法集合。', tags: ['线程', '单例'] },
  ],
  'UninterruptibleThread.scala': [
    { name: 'UninterruptibleThread', kind: 'class', summary: '对中断免疫的线程，保护关键临界区。', tags: ['线程', '中断'] },
  ],
  'UninterruptibleThreadRunner.scala': [
    { name: 'UninterruptibleThreadRunner', kind: 'class', summary: '在不可中断线程中运行指定块。', tags: ['线程', '中断'] },
  ],
  'JsonProtocol.scala': [
    { name: 'JsonProtocol', kind: 'object', summary: 'Spark 事件日志的 JSON 编解码协议。', tags: ['json', '序列化', '单例'] },
  ],
  'HadoopFSUtils.scala': [
    { name: 'HadoopFSUtils', kind: 'object', summary: 'Hadoop FileSystem 操作封装。', tags: ['hadoop', '单例'] },
  ],
  'SizeEstimator.scala': [
    { name: 'SizeEstimator', kind: 'object', summary: '基于反射的对象图大小估算器。', tags: ['内存估算', '单例'] },
  ],
  'IdGenerator.scala': [
    { name: 'IdGenerator', kind: 'class', summary: '单调递增的 ID 生成器。', tags: ['id 生成'] },
  ],
  'MutablePair.scala': [
    { name: 'MutablePair', kind: 'class', summary: '可变的键值对容器。', tags: ['数据结构'] },
  ],
  'NextIterator.scala': [
    { name: 'NextIterator', kind: 'abstract class', summary: '自定义迭代器样板抽象基类。', tags: ['迭代器', '抽象'] },
  ],
  'StatCounter.scala': [
    { name: 'StatCounter', kind: 'class', summary: '在线统计指标（均值/方差/最值）。', tags: ['统计'] },
  ],
  'Distribution.scala': [
    { name: 'Distribution', kind: 'class', summary: '统计分布摘要（含分位数）。', tags: ['统计'] },
  ],
  'CausedBy.scala': [
    { name: 'CausedBy', kind: 'object', summary: '查找异常链根因的工具对象。', tags: ['异常处理', '单例'] },
  ],
  'DependencyUtils.scala': [
    { name: 'DependencyUtils', kind: 'object', summary: '依赖解析工具。', tags: ['依赖管理', '单例'] },
  ],
  'IntParam.scala': [
    { name: 'IntParam', kind: 'class', summary: '可注册到 SparkConf 的整型参数。', tags: ['配置'] },
  ],
  'MemoryParam.scala': [
    { name: 'MemoryParam', kind: 'class', summary: '可注册到 SparkConf 的内存参数（接受 1g/512m 字符串）。', tags: ['配置', '内存'] },
  ],
  'CommandLineUtils.scala': [
    { name: 'CommandLineUtils', kind: 'object', summary: '命令行参数解析工具。', tags: ['命令行', '单例'] },
  ],
  'CollectionsUtils.scala': [
    { name: 'CollectionsUtils', kind: 'object', summary: '集合相关通用工具。', tags: ['集合', '单例'] },
  ],
  'PeriodicCheckpointer.scala': [
    { name: 'PeriodicCheckpointer', kind: 'class', summary: '周期性检查点写入器。', tags: ['检查点', '可靠性'] },
  ],
  'RpcUtils.scala': [
    { name: 'RpcUtils', kind: 'object', summary: 'RPC 框架辅助方法。', tags: ['rpc', '单例'] },
  ],
  'SecurityUtils.scala': [
    { name: 'SecurityUtils', kind: 'object', summary: '安全相关辅助方法。', tags: ['安全', '单例'] },
  ],
  'SerializableBuffer.scala': [
    { name: 'SerializableBuffer', kind: 'class', summary: '可序列化的 NIO ByteBuffer 包装。', tags: ['序列化', 'nio'] },
  ],
  'SerializableConfiguration.scala': [
    { name: 'SerializableConfiguration', kind: 'class', summary: '可序列化的 Hadoop Configuration 包装。', tags: ['序列化', 'hadoop'] },
  ],
  'SerializableJobConf.scala': [
    { name: 'SerializableJobConf', kind: 'class', summary: '可序列化的 Hadoop JobConf 包装。', tags: ['序列化', 'hadoop'] },
  ],
  'SignalUtils.scala': [
    { name: 'SignalUtils', kind: 'object', summary: 'POSIX 信号处理工具。', tags: ['信号', '单例'] },
  ],
  'SparkUncaughtExceptionHandler.scala': [
    { name: 'SparkUncaughtExceptionHandler', kind: 'object', summary: 'Spark 线程未捕获异常统一处理。', tags: ['异常处理', '单例'] },
  ],
  'AccumulatorV2.scala': [
    { name: 'AccumulatorV2', kind: 'abstract class', summary: '累加器 v2 抽象基类。', tags: ['累加器', '抽象'] },
  ],
  'VersionUtils.scala': [
    { name: 'VersionUtils', kind: 'object', summary: '版本号解析与比较工具。', tags: ['版本', '单例'] },
  ],
  'collection/AppendOnlyMap.scala': [
    { name: 'AppendOnlyMap', kind: 'class', summary: '只追加哈希映射，支持 key 上的合并语义。', tags: ['map', '聚合'] },
  ],
  'collection/BitSet.scala': [
    { name: 'BitSet', kind: 'class', summary: '基于 Long[] 的可变位图。', tags: ['位图'] },
  ],
  'collection/CompactBuffer.scala': [
    { name: 'CompactBuffer', kind: 'class', summary: '高效可变缓冲（少元素用数组、否则切缓冲）。', tags: ['缓冲区'] },
  ],
  'collection/ExternalAppendOnlyMap.scala': [
    { name: 'ExternalAppendOnlyMap', kind: 'class', summary: '可溢写磁盘的 AppendOnlyMap。', tags: ['shuffle', 'spill', '聚合'] },
  ],
  'collection/ExternalSorter.scala': [
    { name: 'ExternalSorter', kind: 'class', summary: '可溢写磁盘的外部排序器，sort shuffle 内核。', tags: ['shuffle', 'spill', '排序'] },
  ],
  'collection/ImmutableBitSet.scala': [
    { name: 'ImmutableBitSet', kind: 'class', summary: '不可变位图。', tags: ['位图', '不可变'] },
  ],
  'collection/OpenHashMap.scala': [
    { name: 'OpenHashMap', kind: 'class', summary: '开放寻址高性能哈希映射。', tags: ['map', '哈希'] },
  ],
  'collection/OpenHashSet.scala': [
    { name: 'OpenHashSet', kind: 'class', summary: '开放寻址高性能哈希集合。', tags: ['set', '哈希'] },
  ],
  'collection/PairsWriter.scala': [
    { name: 'PairsWriter', kind: 'abstract class', summary: '写 (K,V) 对的抽象迭代器。', tags: ['shuffle', '迭代器', '抽象'] },
  ],
  'collection/PartitionedAppendOnlyMap.scala': [
    { name: 'PartitionedAppendOnlyMap', kind: 'class', summary: '带分区信息的 AppendOnlyMap。', tags: ['shuffle', '分区', '聚合'] },
  ],
  'collection/PartitionedPairBuffer.scala': [
    { name: 'PartitionedPairBuffer', kind: 'class', summary: '带分区信息的 (K,V) 缓冲。', tags: ['shuffle', '分区', '缓冲'] },
  ],
  'collection/PercentileHeap.scala': [
    { name: 'PercentileHeap', kind: 'class', summary: '近似百分位双堆。', tags: ['百分位', '堆'] },
  ],
  'collection/PrimitiveKeyOpenHashMap.scala': [
    { name: 'PrimitiveKeyOpenHashMap', kind: 'class', summary: '针对基本类型 key 的高性能哈希映射。', tags: ['map', '哈希', '基本类型'] },
  ],
  'collection/PrimitiveVector.scala': [
    { name: 'PrimitiveVector', kind: 'class', summary: '基本类型可变向量。', tags: ['向量'] },
  ],
  'collection/SizeTracker.scala': [
    { name: 'SizeTracker', kind: 'trait', summary: '通过抽样估算集合当前大小。', tags: ['大小估算', 'trait'] },
  ],
  'collection/SizeTrackingAppendOnlyMap.scala': [
    { name: 'SizeTrackingAppendOnlyMap', kind: 'class', summary: '带大小跟踪的 AppendOnlyMap。', tags: ['spill', 'map'] },
  ],
  'collection/SizeTrackingVector.scala': [
    { name: 'SizeTrackingVector', kind: 'class', summary: '带大小跟踪的 PrimitiveVector。', tags: ['spill', '向量'] },
  ],
  'collection/SortDataFormat.scala': [
    { name: 'SortDataFormat', kind: 'abstract class', summary: '排序数据格式抽象。', tags: ['排序', '抽象'] },
  ],
  'collection/Sorter.scala': [
    { name: 'Sorter', kind: 'class', summary: '基于 SortDataFormat 的可溢写排序器。', tags: ['排序'] },
  ],
  'collection/Spillable.scala': [
    { name: 'Spillable', kind: 'abstract class', summary: '可溢写集合基类。', tags: ['spill', '抽象基类'] },
  ],
  'collection/Utils.scala': [
    { name: 'Utils', kind: 'object', summary: 'collection 子包的工具对象。', tags: ['工具对象', '单例'] },
  ],
  'collection/WritablePartitionedPairCollection.scala': [
    { name: 'WritablePartitionedPairCollection', kind: 'trait', summary: '可写出分区 (K,V) 的集合抽象。', tags: ['shuffle', 'trait'] },
  ],
  'io/ChunkedByteBuffer.scala': [
    { name: 'ChunkedByteBuffer', kind: 'class', summary: '分块 NIO ByteBuffer 容器。', tags: ['io', 'nio'] },
  ],
  'io/ChunkedByteBufferFileRegion.scala': [
    { name: 'ChunkedByteBufferFileRegion', kind: 'class', summary: 'ChunkedByteBuffer 到 Netty FileRegion 的适配器。', tags: ['io', '零拷贝', 'netty'] },
  ],
  'io/ChunkedByteBufferOutputStream.scala': [
    { name: 'ChunkedByteBufferOutputStream', kind: 'class', summary: '输出到 ChunkedByteBuffer 的 OutputStream。', tags: ['io', '字节缓冲'] },
  ],
  'logging/DriverLogger.scala': [
    { name: 'DriverLogger', kind: 'object', summary: 'Driver 端结构化日志管理器。', tags: ['日志', 'driver', '单例'] },
  ],
  'logging/FileAppender.scala': [
    { name: 'FileAppender', kind: 'abstract class', summary: '文件日志追加器抽象。', tags: ['日志', '抽象'] },
  ],
  'logging/RollingFileAppender.scala': [
    { name: 'RollingFileAppender', kind: 'class', summary: '滚动文件日志追加器。', tags: ['日志', '滚动'] },
  ],
  'logging/RollingPolicy.scala': [
    { name: 'RollingPolicy', kind: 'trait', summary: '日志滚动策略抽象。', tags: ['日志', '策略', 'trait'] },
  ],
  'random/Pseudorandom.scala': [
    { name: 'Pseudorandom', kind: 'trait', summary: '伪随机数生成器 trait。', tags: ['随机数', 'trait'] },
  ],
  'random/RandomSampler.scala': [
    { name: 'RandomSampler', kind: 'trait', summary: '采样器抽象。', tags: ['采样', 'trait'] },
  ],
  'random/SamplingUtils.scala': [
    { name: 'SamplingUtils', kind: 'object', summary: '采样工具集合。', tags: ['采样', '单例'] },
  ],
  'random/StratifiedSamplingUtils.scala': [
    { name: 'StratifiedSamplingUtils', kind: 'object', summary: '分层采样算法。', tags: ['采样', '分层', '单例'] },
  ],
  'random/XORShiftRandom.scala': [
    { name: 'XORShiftRandom', kind: 'class', summary: 'XORShift 随机数生成器。', tags: ['随机数'] },
  ],
};

// Edges: (fromFile, toFile, edgeType, weight)
const EXTRA_EDGES = [
  // Inheritance
  { from: 'ManualClock.scala', to: 'Clock.scala', type: 'inherits' },
  { from: 'collection/SizeTrackingAppendOnlyMap.scala', to: 'collection/AppendOnlyMap.scala', type: 'inherits' },
  { from: 'collection/SizeTrackingAppendOnlyMap.scala', to: 'collection/SizeTracker.scala', type: 'inherits' },
  { from: 'collection/PartitionedAppendOnlyMap.scala', to: 'collection/SizeTrackingAppendOnlyMap.scala', type: 'inherits' },
  { from: 'collection/PartitionedAppendOnlyMap.scala', to: 'collection/WritablePartitionedPairCollection.scala', type: 'implements' },
  { from: 'collection/PartitionedPairBuffer.scala', to: 'collection/WritablePartitionedPairCollection.scala', type: 'implements' },
  { from: 'collection/PartitionedPairBuffer.scala', to: 'collection/SizeTracker.scala', type: 'inherits' },
  { from: 'collection/ExternalSorter.scala', to: 'collection/Spillable.scala', type: 'inherits' },
  { from: 'collection/ExternalAppendOnlyMap.scala', to: 'collection/Spillable.scala', type: 'inherits' },
  { from: 'collection/Sorter.scala', to: 'collection/SortDataFormat.scala', type: 'depends_on' },
  // depends_on (composition)
  { from: 'collection/ExternalSorter.scala', to: 'collection/PartitionedAppendOnlyMap.scala', type: 'depends_on' },
  { from: 'collection/ExternalSorter.scala', to: 'collection/PartitionedPairBuffer.scala', type: 'depends_on' },
  { from: 'collection/ExternalAppendOnlyMap.scala', to: 'collection/SizeTrackingAppendOnlyMap.scala', type: 'depends_on' },
  // ListenerBus <-> Utils
  { from: 'ListenerBus.scala', to: 'Utils.scala', type: 'depends_on' },
  { from: 'JsonProtocol.scala', to: 'Utils.scala', type: 'depends_on' },
  // ClosureCleaner reflective access to many util types
  { from: 'ClosureCleaner.scala', to: 'SparkFatalException.scala', type: 'depends_on' },
  { from: 'ClosureCleaner.scala', to: 'Utils.scala', type: 'depends_on' },
  // PeriodicCheckpointer depends on Utils
  { from: 'PeriodicCheckpointer.scala', to: 'Utils.scala', type: 'depends_on' },
  // ShutdownHookManager uses ThreadUtils
  { from: 'ShutdownHookManager.scala', to: 'ThreadUtils.scala', type: 'depends_on' },
  // EventLoop uses ThreadUtils
  { from: 'EventLoop.scala', to: 'ThreadUtils.scala', type: 'depends_on' },
  // UninterruptibleThreadRunner uses UninterruptibleThread
  { from: 'UninterruptibleThreadRunner.scala', to: 'UninterruptibleThread.scala', type: 'depends_on' },
  // HadoopFSUtils uses SerializableConfiguration
  { from: 'HadoopFSUtils.scala', to: 'SerializableConfiguration.scala', type: 'depends_on' },
  // io/ChunkedByteBufferFileRegion uses ChunkedByteBuffer
  { from: 'io/ChunkedByteBufferFileRegion.scala', to: 'io/ChunkedByteBuffer.scala', type: 'depends_on' },
  { from: 'io/ChunkedByteBufferOutputStream.scala', to: 'io/ChunkedByteBuffer.scala', type: 'depends_on' },
  // log Slow event uses Utils
  { from: 'ListenerBus.scala', to: 'EventLoop.scala', type: 'related' },
  // collection WritablePartitionedPairCollection implementations
  { from: 'collection/ExternalSorter.scala', to: 'collection/WritablePartitionedPairCollection.scala', type: 'implements' },
  // collection Spillable users
  { from: 'collection/ExternalSorter.scala', to: 'collection/Spillable.scala', type: 'related' },
  // rolling file appender uses rolling policy
  { from: 'logging/RollingFileAppender.scala', to: 'logging/RollingPolicy.scala', type: 'depends_on' },
  { from: 'logging/RollingFileAppender.scala', to: 'logging/FileAppender.scala', type: 'inherits' },
  // random Stratified sampling uses RandomSampler
  { from: 'random/StratifiedSamplingUtils.scala', to: 'random/RandomSampler.scala', type: 'depends_on' },
  { from: 'random/SamplingUtils.scala', to: 'random/RandomSampler.scala', type: 'depends_on' },
  { from: 'random/XORShiftRandom.scala', to: 'random/Pseudorandom.scala', type: 'implements' },
];

const EDGE_WEIGHT = {
  inherits: 0.9, implements: 0.9, depends_on: 0.6, related: 0.5,
  imports: 0.7, documents: 0.5, contains: 1.0, exports: 0.8, calls: 0.8,
};

const allFilePaths = new Set(scan.files.map(f => f.path));

let totalNodes = 0, totalEdges = 0;
for (const batch of batches) {
  const i = batch.batchIndex;
  const nodes = [];
  const edges = [];
  const fileNodeIds = new Set();
  const fileToClassIds = new Map();

  for (const f of batch.files) {
    const totalLines = sizeByPath.get(f.path) || 0;
    const fileNodeId = `file:${f.path}`;
    fileNodeIds.add(fileNodeId);

    let summary, tags;
    if (f.path.endsWith('.scala')) {
      const meta = FILE_META[f.path] || { summary: `${f.path} 的实现。`, tags: ['util'] };
      summary = meta.summary;
      tags = [...meta.tags, 'util'];
    } else if (f.path.endsWith('.md')) {
      summary = `对应 ${f.path.replace(/\.md$/, '.scala').split('/').pop().replace(/\.scala$/, '')} 的 Scaladoc 自动生成 API 参考。`;
      tags = ['文档', 'scaladoc', 'api 参考'];
    } else if (f.path.endsWith('.java')) {
      summary = `${f.path.split('/').pop()} 的 Java 实现，为 Scala 端提供 Java API。`;
      tags = ['java', 'util'];
    } else if (f.path.endsWith('.json')) {
      summary = 'JSON 配置文件。';
      tags = ['配置', 'json'];
    } else {
      summary = '项目元文件。';
      tags = ['元文件'];
    }

    nodes.push({
      id: fileNodeId,
      type: 'file',
      name: f.path.split('/').pop(),
      filePath: f.path,
      summary,
      tags,
      complexity: inferComplexity(totalLines),
    });

    // Class sub-nodes
    if (CLASS_NODES[f.path]) {
      const classIds = [];
      for (const c of CLASS_NODES[f.path]) {
        const prefix = c.kind === 'object' ? 'class' : 'class'; // use 'class' for both
        const cid = `${prefix}:${f.path}:${c.name}`;
        nodes.push({
          id: cid,
          type: 'class',
          name: c.name,
          filePath: f.path,
          summary: c.summary,
          tags: c.tags,
          complexity: inferComplexity(totalLines),
        });
        classIds.push(cid);
        edges.push({
          source: fileNodeId,
          target: cid,
          type: 'contains',
          direction: 'forward',
          weight: 1.0,
        });
        edges.push({
          source: fileNodeId,
          target: cid,
          type: 'exports',
          direction: 'forward',
          weight: 0.8,
        });
      }
      fileToClassIds.set(f.path, classIds);
    }

    // documents edge: md -> scala
    if (f.path.endsWith('.md')) {
      const targetScala = f.path.replace(/\.md$/, '.scala');
      if (allFilePaths.has(targetScala)) {
        edges.push({
          source: fileNodeId,
          target: `file:${targetScala}`,
          type: 'documents',
          direction: 'forward',
          weight: 0.5,
        });
      }
    }
  }

  // Add inheritance/depends_on edges that involve this batch
  for (const e of EXTRA_EDGES) {
    const fromInBatch = batch.files.some(f => f.path === e.from);
    const toInBatch = batch.files.some(f => f.path === e.to);
    if (!fromInBatch || !allFilePaths.has(e.to)) continue;
    edges.push({
      source: `file:${e.from}`,
      target: `file:${e.to}`,
      type: e.type,
      direction: 'forward',
      weight: EDGE_WEIGHT[e.type] || 0.5,
    });
  }

  // Add related: all collection/ files form a subpackage
  // (skip - this is implicit via the layer assignment)

  const out = { nodes, edges };
  writeFileSync(`${OUT_DIR}\\batch-${i}.json`, JSON.stringify(out, null, 2));
  totalNodes += nodes.length;
  totalEdges += edges.length;
  console.log(`Batch ${i}: ${nodes.length} nodes, ${edges.length} edges`);
}

console.log(`Total: ${totalNodes} nodes, ${totalEdges} edges across ${batches.length} batches`);
