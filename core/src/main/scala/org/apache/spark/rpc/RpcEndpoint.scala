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

package org.apache.spark.rpc

import org.apache.spark.SparkException

/**
  * A factory class to create the [[RpcEnv]]. It must have an empty constructor so that it can be
  * created using Reflection.
  */
private[spark] trait RpcEnvFactory {

  def create(config: RpcEnvConfig): RpcEnv
}

/**
  * An end point for the RPC that defines what functions to trigger given a message.
  *
  * It is guaranteed that `onStart`, `receive` and `onStop` will be called in sequence.
  *
  * The life-cycle of an endpoint is:
  *
  * {@code constructor -> onStart -> receive* -> onStop}
  *
  * Note: `receive` can be called concurrently. If you want `receive` to be thread-safe, please use
  * [[ThreadSafeRpcEndpoint]]
  *
  * If any error is thrown from one of [[RpcEndpoint]] methods except `onError`, `onError` will be
  * invoked with the cause. If `onError` throws an error, [[RpcEnv]] will ignore it.
  */
private[spark] trait RpcEndpoint {

  /**
   * The [[RpcEnv]] that this [[RpcEndpoint]] is registered to.
   * 该[[RpcEndpoint]]注册到的[[RpcEnv]]。
   */
  val rpcEnv: RpcEnv

  /**
   * 获取端点引用对象：
   *    当调用“ onStart”时，“ self”将变为有效。 当调用“ onStop”时，“ self”将变为“ null”
   *    在`onStart`之前，[[RpcEndpoint]]尚未注册，并且没有有效的[[RpcEndpointRef]]
   * The [[RpcEndpointRef]] of this [[RpcEndpoint]]. `self` will become valid when `onStart` is
   * called. And `self` will become `null` when `onStop` is called.
   *
   * Note: Because before `onStart`, [[RpcEndpoint]] has not yet been registered and there is not
   * valid [[RpcEndpointRef]] for it. So don't call `self` before `onStart` is called.
   */
  final def self: RpcEndpointRef = {
    //断言，rpcEnv不能为null，否则抛出IllegalArgumentException
    require(rpcEnv != null, "rpcEnv has not been initialized")
    //获取端点引用对象
    rpcEnv.endpointRef(this)
  }

  /**
    * Process messages from `RpcEndpointRef.send` or `RpcCallContext.reply`. If receiving a
    * unmatched message, `SparkException` will be thrown and sent to `onError`.
    */
  def receive: PartialFunction[Any, Unit] = {
    case _ => throw new SparkException(self + " does not implement 'receive'")
  }

  /**
   * Process messages from `RpcEndpointRef.ask`. If receiving a unmatched message,
   * `SparkException` will be thrown and sent to `onError`.
   * 处理来自“ RpcEndpointRef.ask”的消息。 如果收到不匹配的消息，则将抛出“ SparkException”并将其发送给“ onError”。
   * context只是传过来的一个参数，匹配还是匹配PartialFunction参数A
   */
  def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case _ => context.sendFailure(new SparkException(self + " won't reply anything"))
  }

  /**
   * Invoked when any exception is thrown during handling messages.
   * 在处理消息期间引发任何异常时调用。
   */
  def onError(cause: Throwable): Unit = {
    // By default, throw e and let RpcEnv handle it
    throw cause
  }

  /**
   * Invoked when `remoteAddress` is connected to the current node.
   * 当`远端地址`连接到当前节点时调用。
   */
  def onConnected(remoteAddress: RpcAddress): Unit = {
    // By default, do nothing.
  }

  /**
   * Invoked when `remoteAddress` is lost.
   * 当`远端地址`丢失时调用。
   */
  def onDisconnected(remoteAddress: RpcAddress): Unit = {
    // By default, do nothing.
  }

  /**
   * Invoked when some network error happens in the connection between the current node and
   * `remoteAddress`.
    * 当当前节点与“ 远端地址”之间的连接发生网络错误时调用。
   */
  def onNetworkError(cause: Throwable, remoteAddress: RpcAddress): Unit = {
    // By default, do nothing.
  }

  /**
   * Invoked before [[RpcEndpoint]] starts to handle any message.
   * 在[[RpcEndpoint]]开始处理任何消息之前调用。
   */
  def onStart(): Unit = {
    // By default, do nothing.
  }

  /**
   * Invoked when [[RpcEndpoint]] is stopping. `self` will be `null` in this method and you cannot
   * use it to send or ask messages.
   * [[RpcEndpoint]]停止时调用。 在这种方法中，“ self”将为“ null”，并且您不能使用它发送或询问消息。
   */
  def onStop(): Unit = {
    // By default, do nothing.
  }

  /**
   * A convenient method to stop [[RpcEndpoint]].
   * 停止[[RpcEndpoint]]的便捷方法。
   */
  final def stop(): Unit = {
    val _self = self
    if (_self != null) {
      rpcEnv.stop(_self)
    }
  }
}

/**
  * A trait that requires RpcEnv thread-safely sending messages to it.
  *
  * Thread-safety means processing of one message happens before processing of the next message by
  * the same [[ThreadSafeRpcEndpoint]]. In the other words, changes to internal fields of a
  * [[ThreadSafeRpcEndpoint]] are visible when processing the next message, and fields in the
  * [[ThreadSafeRpcEndpoint]] need not be volatile or equivalent.
  *
  * However, there is no guarantee that the same thread will be executing the same
  * [[ThreadSafeRpcEndpoint]] for different messages.
  */
private[spark] trait ThreadSafeRpcEndpoint extends RpcEndpoint
