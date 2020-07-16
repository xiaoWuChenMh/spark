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

package org.apache.spark.util

import java.util.concurrent._

import scala.collection.TraversableLike
import scala.collection.generic.CanBuildFrom
import scala.language.higherKinds

import com.google.common.util.concurrent.{MoreExecutors, ThreadFactoryBuilder}
import scala.concurrent.{Awaitable, ExecutionContext, ExecutionContextExecutor, Future}
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.forkjoin.{ForkJoinPool => SForkJoinPool, ForkJoinWorkerThread => SForkJoinWorkerThread}
import scala.util.control.NonFatal

import org.apache.spark.SparkException

private[spark] object ThreadUtils {

  private val sameThreadExecutionContext =
    ExecutionContext.fromExecutorService(MoreExecutors.sameThreadExecutor())

  /**
   * An `ExecutionContextExecutor` that runs each task in the thread that invokes `execute/submit`.
   * The caller should make sure the tasks running in this `ExecutionContextExecutor` are short and
   * never block.
   */
  def sameThread: ExecutionContextExecutor = sameThreadExecutionContext

  /**
   * Create a thread factory that names threads with a prefix and also sets the threads to daemon.
   * 通过guava jar里的ThreadFactoryBuilder 创建一个线程工厂，该工厂用前缀命名线程，并将线程设置为守护程序。
   */
  def namedThreadFactory(prefix: String): ThreadFactory = {
    new ThreadFactoryBuilder().setDaemon(true).setNameFormat(prefix + "-%d").build()
  }

  /**
   * Wrapper over newCachedThreadPool. Thread names are formatted as prefix-ID, where ID is a
   * unique, sequentially assigned integer.
   */
  def newDaemonCachedThreadPool(prefix: String): ThreadPoolExecutor = {
    val threadFactory = namedThreadFactory(prefix)
    Executors.newCachedThreadPool(threadFactory).asInstanceOf[ThreadPoolExecutor]
  }

  /**
   * 创建一个最大线程数为“ maxThreadNumber”的缓存线程池。 线程名称的格式为前缀ID，其中ID是唯一的，顺序分配的整数。
   */
  def newDaemonCachedThreadPool(
      prefix: String, maxThreadNumber: Int, keepAliveSeconds: Int = 60): ThreadPoolExecutor = {
    //创建一个指定名字的线程工厂
    val threadFactory = namedThreadFactory(prefix)
    //
    val threadPool = new ThreadPoolExecutor(
      maxThreadNumber, // corePoolSize(核心线程池大小)：在排队任务之前要创建的最大线程数
      maxThreadNumber, // maximumPoolSize(最大线程池大小): because we use LinkedBlockingDeque, this one is not used
      keepAliveSeconds,// 线程最大空闲时间
      TimeUnit.SECONDS,// 时间单位
      new LinkedBlockingQueue[Runnable],// 线程等待队列
      threadFactory)// 线程创建工厂
    //该值为true，允许核心线程超时,超时会关闭，即核心线程池的线程在没有任务到达的时候 keepAliveTime时间后关闭
    threadPool.allowCoreThreadTimeOut(true)
    //返回线程池
    threadPool
  }

  /**
   * Wrapper over newFixedThreadPool. Thread names are formatted as prefix-ID, where ID is a unique, sequentially assigned integer.
   * 通过newFixedThreadPool包装。 线程名称的格式为前缀ID，其中ID是唯一的，顺序分配的整数。
   */
  def newDaemonFixedThreadPool(nThreads: Int, prefix: String): ThreadPoolExecutor = {
    //设置一个守护线程的线程工厂
    val threadFactory = namedThreadFactory(prefix)
    //根据上面的线程规程创建一个线程池（守护线程）
    Executors.newFixedThreadPool(nThreads, threadFactory).asInstanceOf[ThreadPoolExecutor]
  }

  /**
   * Wrapper over newSingleThreadExecutor.
   */
  def newDaemonSingleThreadExecutor(threadName: String): ExecutorService = {
    val threadFactory = new ThreadFactoryBuilder().setDaemon(true).setNameFormat(threadName).build()
    Executors.newSingleThreadExecutor(threadFactory)
  }

  /**
   * Wrapper over ScheduledThreadPoolExecutor. 通过ScheduledThreadPoolExecutor进行包装。
   */
  def newDaemonSingleThreadScheduledExecutor(threadName: String): ScheduledExecutorService = {
    //创建一个指定名称的 守护线程工厂
    val threadFactory = new ThreadFactoryBuilder().setDaemon(true).setNameFormat(threadName).build()
    //创建一个可以用于配置定时执行的 线程池
    val executor = new ScheduledThreadPoolExecutor(1, threadFactory)
    // By default, a cancelled task is not automatically removed from the work queue until its delay
    //    // elapses. We have to enable it manually.
    //默认情况下为true，取消的任务不会自动从工作队列中删除，直到其延迟过去。 我们必须手动启用它。
    executor.setRemoveOnCancelPolicy(true)
    //返回线程池
    executor
  }

  /**
   * Wrapper over ScheduledThreadPoolExecutor.
   */
  def newDaemonThreadPoolScheduledExecutor(threadNamePrefix: String, numThreads: Int)
      : ScheduledExecutorService = {
    val threadFactory = new ThreadFactoryBuilder()
      .setDaemon(true)
      .setNameFormat(s"$threadNamePrefix-%d")
      .build()
    val executor = new ScheduledThreadPoolExecutor(numThreads, threadFactory)
    // By default, a cancelled task is not automatically removed from the work queue until its delay
    // elapses. We have to enable it manually.
    executor.setRemoveOnCancelPolicy(true)
    executor
  }

  /**
   * Run a piece of code in a new thread and return the result. Exception in the new thread is
   * thrown in the caller thread with an adjusted stack trace that removes references to this
   * method for clarity. The exception stack traces will be like the following
   *
   * SomeException: exception-message
   *   at CallerClass.body-method (sourcefile.scala)
   *   at ... run in separate thread using org.apache.spark.util.ThreadUtils ... ()
   *   at CallerClass.caller-method (sourcefile.scala)
   *   ...
   */
  def runInNewThread[T](
      threadName: String,
      isDaemon: Boolean = true)(body: => T): T = {
    @volatile var exception: Option[Throwable] = None
    @volatile var result: T = null.asInstanceOf[T]

    val thread = new Thread(threadName) {
      override def run(): Unit = {
        try {
          result = body
        } catch {
          case NonFatal(e) =>
            exception = Some(e)
        }
      }
    }
    thread.setDaemon(isDaemon)
    thread.start()
    thread.join()

    exception match {
      case Some(realException) =>
        // Remove the part of the stack that shows method calls into this helper method
        // This means drop everything from the top until the stack element
        // ThreadUtils.runInNewThread(), and then drop that as well (hence the `drop(1)`).
        val baseStackTrace = Thread.currentThread().getStackTrace().dropWhile(
          ! _.getClassName.contains(this.getClass.getSimpleName)).drop(1)

        // Remove the part of the new thread stack that shows methods call from this helper method
        val extraStackTrace = realException.getStackTrace.takeWhile(
          ! _.getClassName.contains(this.getClass.getSimpleName))

        // Combine the two stack traces, with a place holder just specifying that there
        // was a helper method used, without any further details of the helper
        val placeHolderStackElem = new StackTraceElement(
          s"... run in separate thread using ${ThreadUtils.getClass.getName.stripSuffix("$")} ..",
          " ", "", -1)
        val finalStackTrace = extraStackTrace ++ Seq(placeHolderStackElem) ++ baseStackTrace

        // Update the stack trace and rethrow the exception in the caller thread
        realException.setStackTrace(finalStackTrace)
        throw realException
      case None =>
        result
    }
  }

  /**
   * Construct a new Scala ForkJoinPool with a specified max parallelism and name prefix.
   */
  def newForkJoinPool(prefix: String, maxThreadNumber: Int): SForkJoinPool = {
    // Custom factory to set thread names
    val factory = new SForkJoinPool.ForkJoinWorkerThreadFactory {
      override def newThread(pool: SForkJoinPool) =
        new SForkJoinWorkerThread(pool) {
          setName(prefix + "-" + super.getName)
        }
    }
    new SForkJoinPool(maxThreadNumber, factory,
      null, // handler
      false // asyncMode
    )
  }

  // scalastyle:off awaitresult
  /**
   * Preferred alternative to `Await.result()`.
   *
   * This method wraps and re-throws any exceptions thrown by the underlying `Await` call, ensuring
   * that this thread's stack trace appears in logs.
   *
   * In addition, it calls `Awaitable.result` directly to avoid using `ForkJoinPool`'s
   * `BlockingContext`. Codes running in the user's thread may be in a thread of Scala ForkJoinPool.
   * As concurrent executions in ForkJoinPool may see some [[ThreadLocal]] value unexpectedly, this
   * method basically prevents ForkJoinPool from running other tasks in the current waiting thread.
   * In general, we should use this method because many places in Spark use [[ThreadLocal]] and it's
   * hard to debug when [[ThreadLocal]]s leak to other tasks.
   */
  @throws(classOf[SparkException])
  def awaitResult[T](awaitable: Awaitable[T], atMost: Duration): T = {
    try {
      // `awaitPermission` is not actually used anywhere so it's safe to pass in null here.
      // See SPARK-13747.
      val awaitPermission = null.asInstanceOf[scala.concurrent.CanAwait]
      awaitable.result(atMost)(awaitPermission)
    } catch {
      case e: SparkFatalException =>
        throw e.throwable
      // TimeoutException is thrown in the current thread, so not need to warp the exception.
      case NonFatal(t) if !t.isInstanceOf[TimeoutException] =>
        throw new SparkException("Exception thrown in awaitResult: ", t)
    }
  }
  // scalastyle:on awaitresult

  // scalastyle:off awaitready
  /**
   * Preferred alternative to `Await.ready()`.
   *
   * @see [[awaitResult]]
   */
  @throws(classOf[SparkException])
  def awaitReady[T](awaitable: Awaitable[T], atMost: Duration): awaitable.type = {
    try {
      // `awaitPermission` is not actually used anywhere so it's safe to pass in null here.
      // See SPARK-13747.
      val awaitPermission = null.asInstanceOf[scala.concurrent.CanAwait]
      awaitable.ready(atMost)(awaitPermission)
    } catch {
      // TimeoutException is thrown in the current thread, so not need to warp the exception.
      case NonFatal(t) if !t.isInstanceOf[TimeoutException] =>
        throw new SparkException("Exception thrown in awaitResult: ", t)
    }
  }
  // scalastyle:on awaitready

  def shutdown(
      executor: ExecutorService,
      gracePeriod: Duration = FiniteDuration(30, TimeUnit.SECONDS)): Unit = {
    executor.shutdown()
    executor.awaitTermination(gracePeriod.toMillis, TimeUnit.MILLISECONDS)
    if (!executor.isShutdown) {
      executor.shutdownNow()
    }
  }

  /**
   * Transforms input collection by applying the given function to each element in parallel fashion.
   * Comparing to the map() method of Scala parallel collections, this method can be interrupted
   * at any time. This is useful on canceling of task execution, for example.
   *
   * @param in - the input collection which should be transformed in parallel.
   * @param prefix - the prefix assigned to the underlying thread pool.
   * @param maxThreads - maximum number of thread can be created during execution.
   * @param f - the lambda function will be applied to each element of `in`.
   * @tparam I - the type of elements in the input collection.
   * @tparam O - the type of elements in resulted collection.
   * @return new collection in which each element was given from the input collection `in` by
   *         applying the lambda function `f`.
   */
  def parmap[I, O, Col[X] <: TraversableLike[X, Col[X]]]
      (in: Col[I], prefix: String, maxThreads: Int)
      (f: I => O)
      (implicit
        cbf: CanBuildFrom[Col[I], Future[O], Col[Future[O]]], // For in.map
        cbf2: CanBuildFrom[Col[Future[O]], O, Col[O]] // for Future.sequence
      ): Col[O] = {
    val pool = newForkJoinPool(prefix, maxThreads)
    try {
      implicit val ec = ExecutionContext.fromExecutor(pool)

      val futures = in.map(x => Future(f(x)))
      val futureSeq = Future.sequence(futures)

      awaitResult(futureSeq, Duration.Inf)
    } finally {
      pool.shutdownNow()
    }
  }
}
