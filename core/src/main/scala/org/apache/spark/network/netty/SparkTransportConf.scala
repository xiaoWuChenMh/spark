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

package org.apache.spark.network.netty

import scala.collection.JavaConverters._

import org.apache.spark.SparkConf
import org.apache.spark.network.util.{ConfigProvider, TransportConf}

/**
 * Provides a utility for transforming from a SparkConf inside a Spark JVM (e.g., Executor,
 * Driver, or a standalone shuffle service) into a TransportConf with details on our environment
 * like the number of cores that are allocated to this JVM.
 */
object SparkTransportConf {
  /**
   * Specifies an upper bound on the number of Netty threads that Spark requires by default.
   * In practice, only 2-4 cores should be required to transfer roughly 10 Gb/s, and each core
   * that we use will have an initial overhead of roughly 32 MB of off-heap memory, which comes
   * at a premium.
   *
   * Thus, this value should still retain maximum throughput and reduce wasted off-heap memory
   * allocation. It can be overridden by setting the number of serverThreads and clientThreads
   * manually in Spark's configuration.
   */
  private val MAX_DEFAULT_NETTY_THREADS = 8

  /**
   * Utility for creating a [[TransportConf]] from a [[SparkConf]].
   * 通过一个[[SparkConf]] 来创建一个[[TransportConf]]
   * @param _conf the [[SparkConf]]
   * @param module module名称
   * @param numUsableCores if nonzero, this will restrict the server and client threads to only
   *                       use the given number of cores, rather than all of the machine's cores.
   *                       This restriction will only occur if these properties are not already set.
   *                      可用内核数：如果非零，这将限制服务器和客户端线程仅使用给定数量的内核，而不使用所有计算机内核。
   */
  def fromSparkConf(_conf: SparkConf, module: String, numUsableCores: Int = 0): TransportConf = {
    //克隆一份 SparkConf
    val conf = _conf.clone

    // 获取 Netty客户端和服务器线程池的默认线程数
    val numThreads = defaultNumThreads(numUsableCores)
    //设置rpc 服务端的io 线程个数
    conf.setIfMissing(s"spark.$module.io.serverThreads", numThreads.toString)
    // 设置rpc客户端的io 线程个数
    conf.setIfMissing(s"spark.$module.io.clientThreads", numThreads.toString)

    //初始化TransportConf配置，参数：模块名（rpc）；配置提供者（利用匿名内部类声明的一个抽象类）
    new TransportConf(module, new ConfigProvider {
      //根据配置SparkConf的key获取对应的value。
      override def get(name: String): String = conf.get(name)
      //根据配置SparkConf的key获取对应的value，无值使用默认值
      override def get(name: String, defaultValue: String): String = conf.get(name, defaultValue)
      //获得配置SparkConf中全部的配置迭代器（Map）
      override def getAll(): java.lang.Iterable[java.util.Map.Entry[String, String]] = {
        conf.getAll.toMap.asJava.entrySet()
      }
    })
  }

  /**
   * Returns the default number of threads for both the Netty client and server thread pools.
   * If numUsableCores is 0, we will use Runtime get an approximate number of available cores.
   * 返回Netty客户端和服务器线程池的默认线程数。 如果numUsableCores为0，我们将使用运行时获取可用内核的大约数量。
   */
  private def defaultNumThreads(numUsableCores: Int): Int = {
    //如numUsableCores=0，则使用运行时可以获取的近似的可用内核数，作为客户端和服务端的默认线程数
    val availableCores =
      if (numUsableCores > 0) numUsableCores else Runtime.getRuntime.availableProcessors()
    //可用内核数，不能大于默认的最大网络线程数（8）
    math.min(availableCores, MAX_DEFAULT_NETTY_THREADS)
  }
}
