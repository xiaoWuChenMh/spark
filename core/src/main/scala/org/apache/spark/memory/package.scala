/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark

/**
 * 此软件包实现了 Spark 的内存管理系统。该系统由两个主要 组件组成：一个 JVM 范围的内存管理器和一个每个任务的内存管理器：
 * This package implements Spark's memory management system. This system consists of two main
 * components, a JVM-wide memory manager and a per-task manager:
 *
 *  - [[org.apache.spark.memory.MemoryManager]] manages Spark's overall memory usage within a JVM.
 *    This component implements the policies for dividing the available memory across tasks and for
 *    allocating memory between storage (memory used caching and data transfer) and execution
 *    (memory used by computations, such as shuffles, joins, sorts, and aggregations).
 *    [[org.apache.spark.memory.MemoryManager]] 管理 Spark 在 JVM 中的整体内存使用情况。该组件实现了在各个任务之间分配可用内存的策略，
 *    以及在存储（用于缓存和数据传输的内存）和执行（用于计算的内存，例如 shuffle、join、sort 和 aggregation）之间分配内存的策略。

 *  - [[org.apache.spark.memory.TaskMemoryManager]] manages the memory allocated by individual
 *    tasks. Tasks interact with TaskMemoryManager and never directly interact with the JVM-wide
 *    MemoryManager.
 *  - [[org.apache.spark.memory.TaskMemoryManager]] 管理各个任务分配的内存。任务与 TaskMemoryManager 交互，但不会直接与 JVM 范围的MemoryManager 交互。
 *
 * Internally, each of these components have additional abstractions for memory bookkeeping: 在内部，每个组件都具有用于内存簿记的额外抽象：
 *
 *  - [[org.apache.spark.memory.MemoryConsumer]]s are clients of the TaskMemoryManager and
 *    correspond to individual operators and data structures within a task. The TaskMemoryManager
 *    receives memory allocation requests from MemoryConsumers and issues callbacks to consumers
 *    in order to trigger spilling when running low on memory.
 *  - [[org.apache.spark.memory.MemoryPool]]s are a bookkeeping abstraction used by the
 *    MemoryManager to track the division of memory between storage and execution.
 *
 * Diagrammatically:
 *
 * {{{
 *                                                              +---------------------------+
 *       +-------------+                                        |       MemoryManager       |
 *       | MemConsumer |----+                                   |                           |
 *       +-------------+    |    +-------------------+          |  +---------------------+  |
 *                          +--->| TaskMemoryManager |----+     |  |OnHeapStorageMemPool |  |
 *       +-------------+    |    +-------------------+    |     |  +---------------------+  |
 *       | MemConsumer |----+                             |     |                           |
 *       +-------------+         +-------------------+    |     |  +---------------------+  |
 *                               | TaskMemoryManager |----+     |  |OffHeapStorageMemPool|  |
 *                               +-------------------+    |     |  +---------------------+  |
 *                                                        +---->|                           |
 *                                        *               |     |  +---------------------+  |
 *                                        *               |     |  |OnHeapExecMemPool    |  |
 *       +-------------+                  *               |     |  +---------------------+  |
 *       | MemConsumer |----+                             |     |                           |
 *       +-------------+    |    +-------------------+    |     |  +---------------------+  |
 *                          +--->| TaskMemoryManager |----+     |  |OffHeapExecMemPool   |  |
 *                               +-------------------+          |  +---------------------+  |
 *                                                              |                           |
 *                                                              +---------------------------+
 * }}}
 *
 *
 * There is one implementation of [[org.apache.spark.memory.MemoryManager]]:
 *
 *  - [[org.apache.spark.memory.UnifiedMemoryManager]] enforces soft
 *    boundaries between storage and execution memory, allowing requests for memory in one region
 *    to be fulfilled by borrowing memory from the other.
 */
package object memory
