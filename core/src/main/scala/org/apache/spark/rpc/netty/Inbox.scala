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

package org.apache.spark.rpc.netty

import javax.annotation.concurrent.GuardedBy

import scala.util.control.NonFatal

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcAddress, RpcEndpoint, ThreadSafeRpcEndpoint}


/**定义inobx发送的消息的特质  */
private[netty] sealed trait InboxMessage

/** 单向消息体，继承InboxMessage  ；参数：【发件人地址，消息内容】*/
private[netty] case class OneWayMessage(
    senderAddress: RpcAddress,
    content: Any) extends InboxMessage

/** rpc消息体，继承InboxMessage；参数：【发件人地址，消息内容，Netty Rpc调用上下文】。 */
private[netty] case class RpcMessage(
    senderAddress: RpcAddress,
    content: Any,
    context: NettyRpcCallContext) extends InboxMessage

/** 开始，标志端点数据注册成功   */
private[netty] case object OnStart extends InboxMessage

/** 停止，标志端点数据被注销   */
private[netty] case object OnStop extends InboxMessage

/** A message to tell all endpoints that a remote process has connected.
  * 告诉所有端点远程进程已连接的消息；参数：【远程地址】。
  * */
private[netty] case class RemoteProcessConnected(remoteAddress: RpcAddress) extends InboxMessage

/** A message to tell all endpoints that a remote process has disconnected.
  * 告诉所有端点远程进程已断开连接的消息；参数：【远程地址】。
  *  */
private[netty] case class RemoteProcessDisconnected(remoteAddress: RpcAddress) extends InboxMessage

/** A message to tell all endpoints that a network error has happened.
  * 告诉所有端点发生了网络错误的消息；参数：【异常类，远程地址】。
  * */
private[netty] case class RemoteProcessConnectionError(cause: Throwable, remoteAddress: RpcAddress)
  extends InboxMessage

/**
 * An inbox that stores messages for an [[RpcEndpoint]] and posts messages to it thread-safely.
 * 一个收件箱，用于存储[[RpcEndpoint]]的消息并以线程安全的方式向其发布消息。
  * 参数1：端点引用
  * 参数2：端点
 */
private[netty] class Inbox(
    val endpointRef: NettyRpcEndpointRef,
    val endpoint: RpcEndpoint)
  extends Logging {

  inbox =>  // Give this an alias so we can use it more clearly in closures.

  //待发送的信息容器，是一个LinkedList，内部元素是InboxMessage，InboxMessage封装找待发送的消息
  @GuardedBy("this")
  protected val messages = new java.util.LinkedList[InboxMessage]()

  /** True if the inbox (and its associated endpoint) is stopped.
    * 如果收件箱（及其关联的端点）已停止，则为True。
    * */
  @GuardedBy("this")
  private var stopped = false

  /** Allow multiple threads to process messages at the same time.
    * 是否允许多个线程同时处理消息，默认为false
    * */
  @GuardedBy("this")
  private var enableConcurrent = false

  /** The number of threads processing messages for this inbox.
    * 处理此收件箱消息的线程数
    * */
  @GuardedBy("this")
  private var numActiveThreads = 0

  /**
    * OnStart should be the first message to process
    * OnStart应该是要处理的第一条消息
    */
  inbox.synchronized {
    messages.add(OnStart)
  }

  /**
   * Process stored messages.
   * 处理存储的消息，会一直保持只有一个“numActiveThreads”来处理消息
   */
  def process(dispatcher: Dispatcher): Unit = {
    var message: InboxMessage = null
    //【同步操作】
    inbox.synchronized {
      // 不允许处理多个线程 && 活跃的线程数不为零 ==》退出
      if (!enableConcurrent && numActiveThreads != 0) {
        return
      }
      //从 “待发送的信息容器” 中取出一个消息并从容器中删除
      message = messages.poll()
      //消息不为空，活跃线程+1 ,否则退出
      if (message != null) {
        numActiveThreads += 1
      } else {
        return
      }
    }
    // 通过  while (true)开启无限循环
    while (true) {
      //安全的执行发生逻辑，即加上try ... catch ...
      safelyCall(endpoint) {
        //对待发送的消息执行 ———— 模式匹配
        message match {
            // rpc消息体（发件人地址，消息内容，Netty Rpc调用上下文）
          case RpcMessage(_sender, content, context) =>
            try {
              //执行 端点的 接受且回复信息 的偏函数,
              endpoint.receiveAndReply(context).applyOrElse[Any, Unit](content, { msg =>
                throw new SparkException(s"Unsupported message $message from ${_sender}")
              })
            } catch {
              case e: Throwable =>
                //使用netyy中的channel的writeAndFlush发送一个RpcResponse消息
                context.sendFailure(e)
                //抛出异常，该异常将被safeCall函数捕获，将调用端点的onError函数。
                throw e
            }
            // 单向消息体（发件人地址，消息内容）
          case OneWayMessage(_sender, content) =>
            //执行 端点的 接受信息 偏函数,
            endpoint.receive.applyOrElse[Any, Unit](content, { msg =>
              throw new SparkException(s"Unsupported message $message from ${_sender}")
            })

            //开始，端点初始化 发送的第一个消息
          case OnStart =>
             // 执行 端点的 onStart方法；
            endpoint.onStart()
            //如果 （当前的endpoint不是一个线程安全的端点）执行函数体 内的流程
            if (!endpoint.isInstanceOf[ThreadSafeRpcEndpoint]) {
              inbox.synchronized {
                if (!stopped) {
                  enableConcurrent = true // 设置 允许多个线程同时处理消息
                }
              }
            }
          //结束，当端点注销时会发送一个OnStop消息，也是该inbox执行的最后一个消息
          case OnStop =>
            //获取当前的inbox的活跃线程数
            val activeThreads = inbox.synchronized { inbox.numActiveThreads }
            //执行断言——inbox的活跃线程数只能有一个
            assert(activeThreads == 1,
              s"There should be only a single active thread but found $activeThreads threads.")
            //移除端点引用对象，
            dispatcher.removeRpcEndpointRef(endpoint)
            //停止端点
            endpoint.onStop()
            //执行断言——信箱（message）内容为空，保证OnStop是最后一个消息
            (isEmpty, "OnStop should be the last message")

            //远程进程已连接的消息
          case RemoteProcessConnected(remoteAddress) =>
            //执行 端点的 已连接操作
            endpoint.onConnected(remoteAddress)

           //远程进程已断开连接的消息
          case RemoteProcessDisconnected(remoteAddress) =>
            //执行 端点的 已断开操作
            endpoint.onDisconnected(remoteAddress)
           //发生了网络错误的消息
          case RemoteProcessConnectionError(cause, remoteAddress) =>
            //执行 端点的 网络错误操作
            endpoint.onNetworkError(cause, remoteAddress)
        }
      }

      // 【同步操作】
      inbox.synchronized {
        // 在调用“ onStop”之后，“ enableConcurrent”将设置为false，因此我们应该每次对其进行检查。
        // 条件：不允许处理多个线程 && 活跃的线程数!=1
        if (!enableConcurrent && numActiveThreads != 1) {
          //如果我们不是唯一的worker,将退出 且 活跃线程数-1
          numActiveThreads -= 1
          return
        }
        //从 “待发送的信息容器” 中取出一个消息并从容器中删除
        message = messages.poll()
        //消息为空，活跃线程-1 且 退出
        if (message == null) {
          numActiveThreads -= 1
          return
        }
      }
    }
  }

  /*
  * 【同步操作】 发送消息
  */
  def post(message: InboxMessage): Unit = inbox.synchronized {
    //inbox已标记为 “停止” ，通过onDrop发送warn级别的报错信息
    if (stopped) {
      // We already put "OnStop" into "messages", so we should drop further messages
      onDrop(message)
    } else { //inbox未停止
      //把待发送的消息 放入到 待发送信息容器中，等待发送
      messages.add(message)
      false
    }
  }

  /**
    *【同步操作】 关闭 inbox
    */
  def stop(): Unit = inbox.synchronized {
    // The following codes should be in `synchronized` so that we can make sure "OnStop" is the last message
    // 同步状态，是为了确保“ OnStop”是最后一条消息
    if (!stopped) {
      // We should disable concurrent here. Then when RpcEndpoint.onStop is called, it's the only
      // thread that is processing messages. So `RpcEndpoint.onStop` can release its resources
      // safely.
      //禁用并发
      enableConcurrent = false
      //标记inbox的状态为停止
      stopped = true
      //在 “待发送的信息容器”中放入OnStop,激活停止流程
      messages.add(OnStop)
      // Note: The concurrent events in messages will be processed one by one.
    }
  }

  //【同步操作】 “待发送的信息容器”  是否为空，空返回true
  def isEmpty: Boolean = inbox.synchronized { messages.isEmpty }

  /**
   * Called when we are dropping a message. Test cases override this to test message dropping.
   * Exposed for testing.
   * 在删除消息时调用。 测试用例覆盖此内容以测试消息丢弃。
   * 暴露于测试。
   */
  protected def onDrop(message: InboxMessage): Unit = {
    logWarning(s"Drop $message because $endpointRef is stopped")
  }

  /**
   * Calls action closure, and calls the endpoint's onError function in the case of exceptions.
    * 调用动作闭包，并在发生异常的情况下调用端点的onError函数。
   */
  private def safelyCall(endpoint: RpcEndpoint)(action: => Unit): Unit = {
    // 捕获action函数的异常，符合NonFatal(e)格式，调用端点的onError函数。
    try action catch {
      case NonFatal(e) =>
        try endpoint.onError(e) catch {
          case NonFatal(ee) =>
            if (stopped) {
              logDebug("Ignoring error", ee)
            } else {
              logError("Ignoring error", ee)
            }
        }
    }
  }

}
