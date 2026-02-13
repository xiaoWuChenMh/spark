# DiskBlockManager 类分析文档

## 类的概述和定义

`DiskBlockManager` 是 Apache Spark 存储模块中的核心组件，负责创建和维护逻辑块与物理磁盘位置之间的映射关系。每个逻辑块都映射到一个以 `BlockId` 命名的物理文件，实现了块级别的存储管理。

### 类签名

```scala
private[spark] class DiskBlockManager(
    conf: SparkConf,
    var deleteFilesOnStop: Boolean,
    isDriver: Boolean)
  extends Logging
```

### 核心职责
1. 管理块文件的磁盘存储布局
2. 通过哈希算法将块文件均匀分布到多个本地目录
3. 支持临时块和shuffle块的管理
4. 提供合并shuffle块的支持
5. 管理目录权限，支持安全环境下的shuffle服务
6. 提供文件清理和资源管理功能

## 构造函数参数说明

`DiskBlockManager` 类有三个构造函数参数：

1. **`conf: SparkConf`**
   - **类型**：`SparkConf`
   - **说明**：Spark配置对象，包含所有Spark应用程序的配置参数。DiskBlockManager会从中读取相关的配置项，如本地目录配置、子目录数量、shuffle服务配置等。

2. **`var deleteFilesOnStop: Boolean`**
   - **类型**：`Boolean`（可变变量）
   - **说明**：控制是否在停止时删除文件的标志。当设置为`true`时，DiskBlockManager在停止时会删除所有本地目录中的文件；当设置为`false`时，会保留文件。这个参数可以由`ShuffleDataIO`修改行为。

3. **`isDriver: Boolean`**
   - **类型**：`Boolean`
   - **说明**：标识当前实例是否在Driver端运行。这个参数影响合并shuffle目录的创建逻辑，因为只有Executor端才需要创建合并shuffle的目录。

## 核心属性分析

### 1. 目录相关属性

- **`subDirsPerLocalDir: Int`**
  - 从配置 `config.DISKSTORE_SUB_DIRECTORIES` 获取的每个本地目录下的子目录数量
  - 用于将文件哈希到子目录中，避免顶层目录inode过多

- **`localDirs: Array[File]`**
  - 本地目录数组，根据 `spark.local.dir` 或 `SPARK_LOCAL_DIRS` 配置创建
  - 每个目录都包含 `blockmgr` 子目录
  - 如果创建失败会退出应用程序

- **`localDirsString: Array[String]`**
  - `localDirs` 的字符串表示形式，便于日志和调试

- **`subDirs: Array[Array[File]]`**
  - 二维数组，第一维对应本地目录，第二维对应每个本地目录的子目录
  - 使用同步机制保护每个子目录数组的线程安全

- **`mergeDirName: String`**
  - 合并目录名称，格式为 `merge_manager[_attemptId]`
  - 如果存在应用程序尝试ID，会附加到目录名后

### 2. 权限管理属性

- **`permissionChangingRequired: Boolean`**
  - 判断是否需要修改目录和文件权限的标志
  - 当shuffle服务启用且支持删除shuffle或获取RDD时，需要修改权限
  - 在安全Yarn环境中特别重要，确保shuffle服务能访问文件

### 3. 钩子管理属性

- **`shutdownHook: AnyRef`**
  - 关机钩子引用，用于在JVM关闭时清理资源
  - 优先级为 `TEMP_DIR_SHUTDOWN_PRIORITY + 1`

## 主要方法分类和说明

### 文件获取和查询方法

#### `getFile(filename: String): File`
**方法介绍**：通过哈希算法将文件名映射到本地子目录中的一个文件。这个方法应该与`org.apache.spark.network.shuffle.ExecutorDiskUtils#getFilePath()`保持同步。

**功能概述**：通过双重哈希算法将任意文件名均匀分布到本地目录的子目录中，实现文件存储位置的确定性映射，避免单个目录inode过多，提高文件系统性能。

**详细执行逻辑**：
1. 计算文件名的非负哈希值：`val hash = Utils.nonNegativeHash(filename)`
2. 通过哈希值计算目录ID：`val dirId = hash % localDirs.length` - 确定使用哪个本地目录
3. 计算子目录ID：`val subDirId = (hash / localDirs.length) % subDirsPerLocalDir` - 确定在该本地目录中的哪个子目录
4. 在同步块中创建或获取子目录：
   - 检查`subDirs(dirId)(subDirId)`是否已存在
   - 如果存在则直接使用现有目录
   - 如果不存在则创建新目录：`new File(localDirs(dirId), "%02x".format(subDirId))`
   - 如果目录不存在则创建：`Files.createDirectory(path)`
   - 如果需要权限变更（在安全环境中），将目录设置为组可写：添加`PosixFilePermission.GROUP_WRITE`权限
5. 返回新文件对象：`new File(subDir, filename)`

#### `getFile(blockId: BlockId): File`
**方法介绍**：通过BlockId获取对应的文件，实际上是调用`getFile(blockId.name)`的便捷方法。

**功能概述**：将BlockId转换为对应的物理文件，提供了基于BlockId类型安全访问文件的方法，简化了块文件获取过程。

**详细执行逻辑**：
1. 调用`getFile(blockId.name)`方法

#### `getMergedShuffleFile(blockId: BlockId, dirs: Option[Array[String]]): File`
**方法介绍**：获取合并shuffle块的文件。这个方法应该与`org.apache.spark.network.shuffle.RemoteBlockPushResolver#getFile()`保持同步。

**功能概述**：根据不同的合并shuffle块类型（数据、索引、元数据）获取对应的物理文件，支持推送式shuffle机制，确保与远程块推送解析器保持路径一致性。

**详细执行逻辑**：
1. 根据blockId类型进行模式匹配：
   - 如果是`ShuffleMergedDataBlockId`类型：调用`getMergedShuffleFile(mergedBlockId.name, dirs)`
   - 如果是`ShuffleMergedIndexBlockId`类型：调用`getMergedShuffleFile(mergedIndexBlockId.name, dirs)`
   - 如果是`ShuffleMergedMetaBlockId`类型：调用`getMergedShuffleFile(mergedMetaBlockId.name, dirs)`
   - 如果是其他类型：抛出`IllegalArgumentException`异常

#### `getMergedShuffleFile(filename: String, dirs: Option[Array[String]]): File`
**方法介绍**：私有方法，实际获取合并shuffle文件。

**功能概述**：通过ExecutorDiskUtils工具类生成合并shuffle文件的完整路径，验证目录参数有效性，确保文件路径的正确生成和标准化。

**详细执行逻辑**：
1. 检查目录参数：`if (!dirs.exists(_.nonEmpty))`
   - 如果目录为空或不存在：抛出`IllegalArgumentException`异常
2. 调用`ExecutorDiskUtils.getFilePath(dirs.get, subDirsPerLocalDir, filename)`获取文件路径
3. 返回新的`File`对象

#### `containsBlock(blockId: BlockId): Boolean`
**方法介绍**：检查磁盘块管理器是否包含指定的块。

**功能概述**：快速检查指定块是否已存储在磁盘上，通过验证对应物理文件的存在性来判断块存储状态，用于块存在性查询和状态验证。

**详细执行逻辑**：
1. 调用`getFile(blockId.name)`获取文件
2. 检查文件是否存在：`.exists()`

### 文件列表和块管理方法

#### `getAllFiles(): Seq[File]`
**方法介绍**：列出磁盘管理器当前存储的所有文件。

**功能概述**：递归扫描所有子目录获取当前存储的全部物理文件列表，使用线程安全的目录复制避免并发修改问题，提供磁盘存储状态的完整视图。

**详细执行逻辑**：
1. 遍历所有子目录数组：`subDirs.flatMap { dir =>`
2. 在每个目录的同步块中复制目录内容：`dir.synchronized { dir.clone() }`
3. 过滤掉null值：`.filter(_ != null)`
4. 遍历每个目录获取文件列表：`.flatMap { dir =>`
5. 获取目录中的所有文件：`val files = dir.listFiles()`
6. 如果文件不为null则转换为Seq，否则返回空Seq：`if (files != null) files.toSeq else Seq.empty`

#### `getAllBlocks(): Seq[BlockId]`
**方法介绍**：列出磁盘管理器当前存储的所有块。

**功能概述**：将磁盘上所有物理文件转换为对应的BlockId列表，过滤掉无法识别的非块文件（如临时文件），提供逻辑块的完整清单用于存储管理和监控。

**详细执行逻辑**：
1. 调用`getAllFiles()`获取所有文件
2. 遍历每个文件尝试转换为BlockId：`.flatMap { f =>`
3. 尝试创建BlockId：`try { Some(BlockId(f.getName)) }`
4. 如果遇到无法识别的BlockId：`catch { case _: UnrecognizedBlockId => None }`
   - 跳过不对应块的文件（如`SortShuffleWriter`创建的临时文件）

### 文件创建和权限管理方法

#### `createWorldReadableFile(file: File): Unit`
**方法介绍**：确保文件创建为全局可读。这是为了解决在安全Yarn环境中，将块管理器子目录设置为组可写时会移除setgid位的问题，否则shuffle服务无法读取shuffle文件。外层目录仍然不是全局可执行的，因此只有运行用户和shuffle服务可以访问这些文件。

**功能概述**：创建具有全局可读权限的文件，解决安全Yarn环境中shuffle服务无法访问shuffle文件的问题，通过添加OTHERS_READ权限确保shuffle服务能读取文件，同时保持安全控制。

**详细执行逻辑**：
1. 获取文件路径：`val path = file.toPath`
2. 创建文件：`Files.createFile(path)`
3. 获取当前权限：`val currentPerms = Files.getPosixFilePermissions(path)`
4. 添加全局读权限：`currentPerms.add(PosixFilePermission.OTHERS_READ)`
5. 设置新权限：`Files.setPosixFilePermissions(path, currentPerms)`

#### `createTempFileWith(file: File): File`
**方法介绍**：创建给定文件的临时版本，具有全局可读权限（如果需要）。用于创建将被重命名为最终版本文件的块文件。

**功能概述**：生成用于原子写入的临时文件，在安全环境中自动设置全局可读权限，支持安全的原子文件写入模式，避免写入过程中的文件状态不一致。

**详细执行逻辑**：
1. 创建临时文件：`val tmpFile = Utils.tempFileWith(file)`
2. 检查是否需要权限变更：`if (permissionChangingRequired)`
3. 如果需要权限变更，调用`createWorldReadableFile(tmpFile)`
4. 返回临时文件

### 临时块创建方法

#### `createTempLocalBlock(): (TempLocalBlockId, File)`
**方法介绍**：生成唯一的块ID和适合存储本地中间结果的File。

**功能概述**：创建唯一且不冲突的本地临时块及其存储文件，通过UUID确保块ID的唯一性，避免文件名冲突，支持本地中间结果的临时存储。

**详细执行逻辑**：
1. 生成初始块ID：`var blockId = new TempLocalBlockId(UUID.randomUUID())`
2. 循环检查块是否已存在：`while (getFile(blockId).exists())`
3. 如果已存在，重新生成块ID：`blockId = new TempLocalBlockId(UUID.randomUUID())`
4. 返回块ID和对应的文件：`(blockId, getFile(blockId))`

#### `createTempShuffleBlock(): (TempShuffleBlockId, File)`
**方法介绍**：生成唯一的块ID和适合存储shuffle中间结果的File。

**功能概述**：创建唯一且不冲突的shuffle临时块及其存储文件，确保在安全环境中自动设置全局可读权限，支持shuffle中间结果的临时存储和shuffle服务访问。

**详细执行逻辑**：
1. 生成初始块ID：`var blockId = new TempShuffleBlockId(UUID.randomUUID())`
2. 循环检查块是否已存在：`while (getFile(blockId).exists())`
3. 如果已存在，重新生成块ID：`blockId = new TempShuffleBlockId(UUID.randomUUID())`
4. 获取临时文件：`val tmpFile = getFile(blockId)`
5. 检查是否需要权限变更：`if (permissionChangingRequired)`
6. 如果需要权限变更，调用`createWorldReadableFile(tmpFile)`
7. 返回块ID和临时文件：`(blockId, tmpFile)`

### 目录创建方法

#### `createLocalDirs(conf: SparkConf): Array[File]`
**方法介绍**：创建用于存储块数据的本地目录。这些目录位于配置的本地目录内，使用外部shuffle服务时不会在JVM退出时删除。

**功能概述**：根据Spark配置创建块管理器所需的本地目录结构，包含容错机制和日志记录，确保块数据存储位置可用性，支持外部shuffle服务场景。

**详细执行逻辑**：
1. 获取配置的本地目录：`Utils.getConfiguredLocalDirs(conf).flatMap { rootDir =>`
2. 尝试创建目录：`try {`
3. 创建本地目录：`val localDir = Utils.createDirectory(rootDir, "blockmgr")`
4. 记录日志：`logInfo(s"Created local directory at $localDir")`
5. 返回Some(localDir)
6. 如果出现IO异常：`catch { case e: IOException =>`
7. 记录错误日志：`logError(s"Failed to create local dir in $rootDir. Ignoring this directory.", e)`
8. 返回None

#### `createLocalDirsForMergedShuffleBlocks(): Unit`
**方法介绍**：如果启用了基于推送的shuffle，获取存储由executor创建的合并shuffle块的配置本地目录列表。注意，此目录中的文件将由外部shuffle服务创建。我们只在这里创建merge_manager目录和子目录，因为目前外部shuffle服务没有权限在应用程序本地目录下创建目录。

**功能概述**：在启用推送式shuffle时创建专门的合并shuffle目录结构，包含具有适当权限的子目录，确保外部shuffle服务能够访问和创建合并shuffle文件。

**详细执行逻辑**：
1. 检查是否启用了基于推送的shuffle：`if (Utils.isPushBasedShuffleEnabled(conf, isDriver = isDriver, checkSerializer = false))`
2. 遍历所有配置的本地目录：`Utils.getConfiguredLocalDirs(conf).foreach { rootDir =>`
3. 尝试创建合并目录：`try {`
4. 创建合并目录对象：`val mergeDir = new File(rootDir, mergeDirName)`
5. 检查目录是否存在或子目录数量不足：`if (!mergeDir.exists() || mergeDir.listFiles().length < subDirsPerLocalDir)`
6. 记录调试日志：`logDebug(s"Try to create $mergeDir and its sub dirs since the $mergeDirName dir does not exist")`
7. 循环创建子目录：`for (dirNum <- 0 until subDirsPerLocalDir)`
8. 创建子目录对象：`val subDir = new File(mergeDir, "%02x".format(dirNum))`
9. 检查子目录是否存在：`if (!subDir.exists())`
10. 调用`createDirWithPermission770(subDir)`创建目录
11. 记录信息日志：`logInfo(s"Merge directory and its sub dirs get created at $mergeDir")`
12. 如果出现IO异常：`catch { case e: IOException =>`
13. 记录错误日志：`logError(s"Failed to create $mergeDirName dir in $rootDir. Ignoring this directory.", e)`

#### `createDirWithPermission770(dirToCreate: File): Unit`
**方法介绍**：创建组可写的目录。授予目录770 "rwxrwx---"权限，以便shuffle服务器可以在合并文件夹内创建子目录/文件。

**功能概述**：创建具有770权限（rwxrwx---）的目录，确保shuffle服务器能够访问和操作目录内容，包含重试机制和异常处理，保证目录创建的成功率和可靠性。

**详细执行逻辑**：
1. 初始化尝试次数：`var attempts = 0`
2. 获取最大尝试次数：`val maxAttempts = Utils.MAX_DIR_CREATION_ATTEMPTS`
3. 初始化创建标志：`var created: File = null`
4. 循环尝试创建：`while (created == null)`
5. 增加尝试次数：`attempts += 1`
6. 检查是否超过最大尝试次数：`if (attempts > maxAttempts)`
7. 如果超过，抛出创建目录错误：`throw SparkCoreErrors.failToCreateDirectoryError(dirToCreate.getAbsolutePath, maxAttempts)`
8. 尝试创建目录：`try {`
9. 创建目录：`dirToCreate.mkdirs()`
10. 设置权限：`Files.setPosixFilePermissions(dirToCreate.toPath, PosixFilePermissions.fromString("rwxrwx---"))`
11. 检查目录是否存在：`if (dirToCreate.exists())`
12. 设置创建标志：`created = dirToCreate`
13. 记录调试日志：`logDebug(s"Created directory at ${dirToCreate.getAbsolutePath} with permission 770")`
14. 如果出现安全异常：`catch { case e: SecurityException =>`
15. 记录警告日志：`logWarning(s"Failed to create directory ${dirToCreate.getAbsolutePath} with permission 770", e)`
16. 重置创建标志：`created = null`

### 元数据管理方法

#### `getMergeDirectoryAndAttemptIDJsonString(): String`
**方法介绍**：获取合并目录和尝试ID的JSON字符串表示。

**功能概述**：生成包含合并目录名称和应用程序尝试ID的JSON格式元数据字符串，用于序列化和传递合并shuffle目录配置信息，支持分布式环境下的配置同步。

**详细执行逻辑**：
1. 创建元数据Map：`val mergedMetaMap: HashMap[String, String] = new HashMap[String, String]()`
2. 添加合并目录键值：`mergedMetaMap.put(MERGE_DIR_KEY, mergeDirName)`
3. 如果有尝试ID，添加到Map：`conf.get(config.APP_ATTEMPT_ID).foreach(attemptId => mergedMetaMap.put(ATTEMPT_ID_KEY, attemptId))`
4. 创建ObjectMapper：`val mapper = new ObjectMapper()`
5. 注册Scala模块：`mapper.registerModule(DefaultScalaModule)`
6. 将Map转换为JSON字符串：`val jsonString = mapper.writeValueAsString(mergedMetaMap)`
7. 返回JSON字符串

### 关闭和清理方法

#### `addShutdownHook(): AnyRef`
**方法介绍**：添加关机钩子。

**功能概述**：注册JVM关机钩子，确保在应用程序终止时能够正确清理磁盘资源，使用适当的优先级确保在临时目录清理前执行，实现可靠的资源管理。

**详细执行逻辑**：
1. 记录调试日志（强制提前创建logger）：`logDebug("Adding shutdown hook")`
2. 添加关机钩子：`ShutdownHookManager.addShutdownHook(ShutdownHookManager.TEMP_DIR_SHUTDOWN_PRIORITY + 1) { () =>`
3. 记录信息日志：`logInfo("Shutdown hook called")`
4. 调用`DiskBlockManager.this.doStop()`
5. 返回钩子引用

#### `stop(): Unit`
**方法介绍**：清理本地目录并停止shuffle发送器。

**功能概述**：执行DiskBlockManager的停止流程，移除之前注册的关机钩子并触发实际的清理操作，包含异常处理和日志记录，确保资源的正确释放。

**详细执行逻辑**：
1. 移除关机钩子：`try { ShutdownHookManager.removeShutdownHook(shutdownHook) }`
2. 如果出现异常：`catch { case e: Exception =>`
3. 记录错误日志：`logError(s"Exception while removing shutdown hook.", e)`
4. 调用`doStop()`

#### `doStop(): Unit`
**方法介绍**：执行实际的停止逻辑。

**功能概述**：根据deleteFilesOnStop配置决定是否删除所有本地存储目录，包含目录存在性检查和安全删除机制，避免删除系统关键目录，实现可控的存储清理。

**详细执行逻辑**：
1. 检查是否需要删除文件：`if (deleteFilesOnStop)`
2. 遍历所有本地目录：`localDirs.foreach { localDir =>`
3. 检查是否是目录且存在：`if (localDir.isDirectory() && localDir.exists())`
4. 尝试删除：`try {`
5. 检查目录是否在关机删除目录列表中：`if (!ShutdownHookManager.hasRootAsShutdownDeleteDir(localDir))`
6. 递归删除目录：`Utils.deleteRecursively(localDir)`
7. 如果出现异常：`catch { case e: Exception =>`
8. 记录错误日志：`logError(s"Exception while deleting local spark dir: $localDir", e)`

## 设计特点总结

### 1. 目录哈希设计
- 使用双重哈希策略：先哈希到本地目录，再哈希到子目录
- 避免单个目录inode过多，提高文件系统性能
- 使用`Utils.nonNegativeHash`确保哈希值非负

### 2. 线程安全设计
- 使用`synchronized`保护子目录数组的修改
- 在`getAllFiles()`中复制目录内容避免并发修改问题
- 目录创建使用文件系统处理的竞态条件

### 3. 权限管理设计
- 支持安全环境下的shuffle服务访问
- 通过设置组可写和全局可读权限平衡安全性和功能性
- SPARK-37618修复：解决setgid位丢失问题

### 4. 资源管理设计
- 使用关机钩子确保资源清理
- 支持可配置的文件删除行为
- 优雅的错误处理和日志记录

### 5. 模块化设计
- 分离本地块和shuffle块管理
- 支持合并shuffle块的特殊处理
- 配置驱动的行为控制

## 配置参数说明

### 核心配置参数

1. **`spark.local.dir` / `SPARK_LOCAL_DIRS`**
   - **作用**：指定本地目录路径，用于存储块文件
   - **默认值**：系统临时目录
   - **影响**：决定`localDirs`的创建位置

2. **`spark.diskStore.subDirectories`**
   - **配置项**：`config.DISKSTORE_SUB_DIRECTORIES`
   - **作用**：指定每个本地目录下的子目录数量
   - **默认值**：64
   - **影响**：决定`subDirsPerLocalDir`的值，影响文件哈希分布

3. **`spark.app.attempt.id`**
   - **配置项**：`config.APP_ATTEMPT_ID`
   - **作用**：应用程序尝试ID
   - **影响**：合并目录名称的后缀，支持多次尝试

### Shuffle服务相关配置

4. **`spark.shuffle.service.enabled`**
   - **配置项**：`config.SHUFFLE_SERVICE_ENABLED`
   - **作用**：是否启用外部shuffle服务
   - **影响**：决定是否需要权限变更

5. **`spark.shuffle.service.removeShuffle.enabled`**
   - **配置项**：`config.SHUFFLE_SERVICE_REMOVE_SHUFFLE_ENABLED`
   - **作用**：是否允许shuffle服务删除shuffle文件
   - **影响**：决定是否需要权限变更

6. **`spark.shuffle.service.fetch.rdd.enabled`**
   - **配置项**：`config.SHUFFLE_SERVICE_FETCH_RDD_ENABLED`
   - **作用**：是否允许shuffle服务获取RDD块
   - **影响**：决定是否需要权限变更

### 推送式shuffle配置

7. **推送式shuffle启用标志**
   - **检测方法**：`Utils.isPushBasedShuffleEnabled()`
   - **作用**：决定是否创建合并shuffle目录
   - **影响**：`createLocalDirsForMergedShuffleBlocks()`的执行

## 扩展内容

### 性能优化点分析

1. **目录哈希优化**
   - 使用简单的取模运算，计算效率高
   - 双重哈希减少目录冲突
   - 子目录使用十六进制命名（"%02x"），目录数量可控

2. **懒加载子目录**
   - 子目录在首次需要时才创建
   - 减少不必要的目录创建开销
   - 同步块内创建避免竞态条件

3. **批量文件操作**
   - `getAllFiles()`使用`flatMap`和`filter`链式操作
   - 避免中间集合的多次创建
   - 使用`clone()`保护并发访问

### 异常处理机制

1. **目录创建异常**
   - `createLocalDirs()`捕获`IOException`，记录错误并忽略失败目录
   - `createLocalDirsForMergedShuffleBlocks()`类似处理
   - 确保部分失败不影响整体功能

2. **块ID解析异常**
   - `getAllBlocks()`捕获`UnrecognizedBlockId`，跳过非块文件
   - 支持临时文件（如`SortShuffleWriter`创建的文件）共存

3. **权限设置异常**
   - `createDirWithPermission770()`捕获`SecurityException`，记录警告并重试
   - 支持最大尝试次数，避免无限循环

### 与其他模块的交互关系

1. **与`BlockManager`的关系**
   - `DiskBlockManager`是`BlockManager`的磁盘存储部分
   - 负责物理文件的映射和管理
   - `BlockManager`负责逻辑块的管理和内存/磁盘协调

2. **与`ExecutorDiskUtils`的关系**
   - `getFile()`方法与`ExecutorDiskUtils.getFilePath()`保持同步
   - 确保网络shuffle和本地存储使用相同的文件路径算法

3. **与`RemoteBlockPushResolver`的关系**
   - `getMergedShuffleFile()`与`RemoteBlockPushResolver.getFile()`保持同步
   - 支持推送式shuffle的块文件定位

4. **与`ShuffleService`的关系**
   - 权限管理专门为外部shuffle服务设计
   - 支持shuffle服务在安全环境中访问和删除文件

### 使用场景和最佳实践建议

1. **生产环境配置**
   - 设置多个`spark.local.dir`路径，分布在不同的物理磁盘
   - 提高IO并行度和容错能力
   - 根据文件数量调整`spark.diskStore.subDirectories`

2. **安全环境部署**
   - 启用外部shuffle服务时，确保目录权限正确设置
   - 监控权限变更失败日志，及时处理权限问题
   - 测试shuffle服务对文件的读写权限

3. **性能监控**
   - 监控本地目录空间使用情况
   - 观察子目录创建日志，确保目录结构正常
   - 检查文件删除操作的成功率

4. **故障排查**
   - 如果出现目录创建失败，检查磁盘空间和权限
   - 如果shuffle服务无法读取文件，检查权限设置
   - 使用`getAllFiles()`和`getAllBlocks()`诊断存储状态