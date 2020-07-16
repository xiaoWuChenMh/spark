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

import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap, LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}
import javax.annotation.concurrent.GuardedBy

import scala.collection.JavaConverters._
import scala.concurrent.Promise
import scala.util.control.NonFatal

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.network.client.RpcResponseCallback
import org.apache.spark.rpc._
import org.apache.spark.util.ThreadUtils

/**
 * A message dispatcher, responsible for routing RPC messages to the appropriate endpoint(s).
 * 收件信箱的调度器：每一个端点对应一个端点引用，同时对应着一个inbox
 *
 * @param numUsableCores 分配给进程的CPU内核数，用于调整线程池的大小。 如果为0，将考虑主机上的可用CPU。
 */
private[netty] class Dispatcher(nettyEnv: NettyRpcEnv, numUsableCores: Int) extends Logging {

  private class EndpointData(
      val name: String,
      val endpoint: RpcEndpoint,
      val ref: NettyRpcEndpointRef) {
    //根据端点和端点引用创建一个inbox
    val inbox = new Inbox(ref, endpoint) //初始化时会发送 OnStart
  }

  //rpc端点容器，线程安全的hashMap,key为endpoint名称，value为封装endpoint名称, endpoint,endpointRef的类EndpointData
  private val endpoints: ConcurrentMap[String, EndpointData] =
    new ConcurrentHashMap[String, EndpointData]

  //rpc端点引用容器，线程安全的hashMap,key为endpoint，value为对应的endpointRef
  private val endpointRefs: ConcurrentMap[RpcEndpoint, RpcEndpointRef] =
    new ConcurrentHashMap[RpcEndpoint, RpcEndpointRef]

  // Track the receivers whose inboxes may contain messages.
  // 接受者容器，是一个阻塞队列，跟踪收件箱中可能包含消息的接收器(消息放入)
  private val receivers = new LinkedBlockingQueue[EndpointData]

  /**
   * True if the dispatcher has been stopped. Once stopped, all messages posted will be bounced
   * immediately.
   * 如果调度程序已停止，则为true。 一旦停止，所有发送的信息将立即被退回。
   */
  @GuardedBy("this")
  private var stopped = false

  /*
  * @Description:  @Description:  注册Endpoint,仅追踪，不发送任何消息，端点代指endpoint
  * @Param:name     rpc端点的名字
  * @Param:endpoint rpc端点的实例对象
  * @return: rpc端点引用
  */
  def registerRpcEndpoint(name: String, endpoint: RpcEndpoint): NettyRpcEndpointRef = {
    //RPC 端点的地址
    val addr = RpcEndpointAddress(nettyEnv.address, name)
    //RPC 端点的引用，包括：nettyEnv配置信息、端点地址、nettyEnv
    val endpointRef = new NettyRpcEndpointRef(nettyEnv.conf, addr, nettyEnv)
    synchronized {
      //如果dispatcher已停止工作，抛出错误“非法状态异常”
      if (stopped) {
        throw new IllegalStateException("RpcEnv has been stopped")
      }
      //如果以当前rpc端点名字作为key的数据已存入rpc端点容器中，抛出错误“有争议的非法异常”
      if (endpoints.putIfAbsent(name, new EndpointData(name, endpoint, endpointRef)) != null) {
        throw new IllegalArgumentException(s"There is already an RpcEndpoint called $name")
      }
      //从rpc端点容器 获取封装端点相关信息的实例对象EndpointData
      val data = endpoints.get(name)
      //将当前rpc端点及其引用信息放入到 rpc端点引用容器中
      endpointRefs.put(data.endpoint, data.ref)
      //将当前的EndpointData 放入 阻塞队列追踪器
      receivers.offer(data)  // for the OnStart message
    }
    //返回当前rpc端点对应的引用
    endpointRef
  }

  /*
  * @Description:获取端点引用对象， 根据端点实例对象从rpc端点引用容器（map）中查找对应的引用
  * @Param: rpc端点的实例对象
  * @return: rpc端点引用
  */
  def getRpcEndpointRef(endpoint: RpcEndpoint): RpcEndpointRef = endpointRefs.get(endpoint)

  /*
  * @Description:移除端点引用对象， 根据端点实例对象从rpc端点引用容器（map）中移除对应的引用
  * @Param: rpc端点的实例对象
  * @return: 无返回
  */
  def removeRpcEndpointRef(endpoint: RpcEndpoint): Unit = endpointRefs.remove(endpoint)

  /*
  * @Description: Should be idempotent
  *                根据rpc端点的名字注销对应的rpc端点，具体操作：删除rpc端点容器对应的端点信息 and 发生停止信息 and 当前端点信息放入追踪容器
  * @Param: rpc端点的名字
  * @return: 无返回
  */
  private def unregisterRpcEndpoint(name: String): Unit = {
    //挺过端点名，从以注册的端点容器中移除 对应的端点信息，并返回被移除信息
    val data = endpoints.remove(name)
    if (data != null) {
      //停止inbox，实际上将一个OnStop信息投递到了inbox的信箱（messages）中
      data.inbox.stop()
      //
      receivers.offer(data)  // for the OnStop message
    }
    // 不在此处清理`endpointRefs`，因可能有获正在处理的消息，`endpointRefs`会在Inbox中被清楚
    // Don't clean `endpointRefs` here because it's possible that some messages are being processed
    // now and they can use `getRpcEndpointRef`. So `endpointRefs` will be cleaned in Inbox via
    // `removeRpcEndpointRef`.
  }

  /*
  * @Description: 通过rpd端点引用，来注销停止rpd端点
  */
  def stop(rpcEndpointRef: RpcEndpointRef): Unit = {
    //加锁
    synchronized {
      if (stopped) { //Dispatcher（调度器）已停止，不做任何操作，因为在stop()方法所有有效的端点都逐一被注销。
        return
      }
      //根据名称注销指定的rpc端点
      unregisterRpcEndpoint(rpcEndpointRef.name)
    }
  }

  /**
   * Send a message to all registered [[RpcEndpoint]]s in this process.
   * 在这个进程中，向所有的已注册的rpc端点发送消息
   *
   * This can be used to make network events known to all end points (e.g. "a new node connected").
   * 这可用于使 “网络事件” 为所有端点所知（例如“已连接的新节点”）。
   */
  def postToAll(message: InboxMessage): Unit = {
    //获取所有的endpoint名称 的迭代器
    val iter = endpoints.keySet().iterator()
    //遍历迭代器
    while (iter.hasNext) {
      val name = iter.next
        //调用消息投递方法
        postMessage(name, message, (e) => { e match {
          case e: RpcEnvStoppedException => logDebug (s"Message $message dropped. ${e.getMessage}")
          case e: Throwable => logWarning(s"Message $message dropped. ${e.getMessage}")
        }}
      )}
  }

  /** Posts a message sent by a remote endpoint.
    * 远程端点投递的消息
    * callback：在 TransportRequestHandler.processRpcRequest 中有一个实例化对象
    * */
  def postRemoteMessage(message: RequestMessage, callback: RpcResponseCallback): Unit = {
    //远程的Netty Rpc调用上下文，包含：nettyEnv,回调，发件人地址
    val rpcCallContext =
      new RemoteNettyRpcCallContext(nettyEnv, callback, message.senderAddress)
    //封装成一个待投递的消息————InboxMessage，类型是RpcMessage
    val rpcMessage = RpcMessage(message.senderAddress, message.content, rpcCallContext)
    //调用消息投递方法
    postMessage(message.receiver.name, rpcMessage, (e) => callback.onFailure(e))
  }

  /** Posts a message sent by a local endpoint.
    * 本地端点投递过来的消息
    * */
  def postLocalMessage(message: RequestMessage, p: Promise[Any]): Unit = {
    //本地的Netty rpc调用上下文，包含：发件人地址,Promise
    val rpcCallContext =
      new LocalNettyRpcCallContext(message.senderAddress, p)
    //封装成一个待投递的消息————InboxMessage，类型是RpcMessage
    val rpcMessage = RpcMessage(message.senderAddress, message.content, rpcCallContext)
    //调用消息投递方法
    postMessage(message.receiver.name, rpcMessage, (e) => p.tryFailure(e))
  }

  /** Posts a one-way message.
    * 一个单向的消息投递,不需要回调
    * */
  def postOneWayMessage(message: RequestMessage): Unit = {
    //调用消息投递方法
    postMessage(message.receiver.name, OneWayMessage(message.senderAddress, message.content),
      (e) => throw e)
  }

  /**
   * 将消息投递到指定的rpc端点的inbox,触发消息处理逻辑
   *
   * @param endpointName 端点名
   * @param message 具体要发送的消息
   * @param callbackIfStopped 端点停止时的回调函数
   */
  private def postMessage(
      endpointName: String,
      message: InboxMessage,
      callbackIfStopped: (Exception) => Unit): Unit = {
    val error = synchronized {
      //根据端点名从端点容器中获取对应的端点数据
      val data = endpoints.get(endpointName)
      if (stopped) { //dispatcher的状态为停止，返回rpc环境停止异常
        Some(new RpcEnvStoppedException())
      } else if (data == null) { //端点名所对应的端点数据为空，返回SparkException异常
        Some(new SparkException(s"Could not find $endpointName."))
      } else {
        //将待发送的消息放入 端点所对应的inbox中的 待发送信息容器（messages）中
        data.inbox.post(message)
        //将该端点对应的EndpointData放入到追踪队列中
        receivers.offer(data)
        None
      }
    }
    // We don't need to call `onStop` in the `synchronized` block
    // 通过回调方法处理异常信息
    error.foreach(callbackIfStopped)
  }

  /*
  * @Description: 停止Dispatcher
  */
  def stop(): Unit = {
    //加锁
    synchronized {
      // 如果以停止，直接退出
      if (stopped) {
        return
      }
      // 设为true，代表调度程序已停止。
      stopped = true
    }
    // Stop all endpoints. This will queue all endpoints for processing by the message loops.
    // 停止所有端点。 这会将所有端点排队，以便由消息循环进行处理。
    endpoints.keySet().asScala.foreach(unregisterRpcEndpoint)
    // 把可以使消息循环体（MessageLoop）停止的“毒药”放入到 “接受者容器”中
    receivers.offer(PoisonPill)
    //停止线程池
    threadpool.shutdown()
  }

  /*
  * 线程池等待终止时间
  *  用于设定超时时间及单位,当等待超过设定时间时，会监测ExecutorService是否已经关闭，若关闭则返回true，否则返回false。
  */
  def awaitTermination(): Unit = {
    threadpool.awaitTermination(Long.MaxValue, TimeUnit.MILLISECONDS)
  }

  /**
   * Return if the endpoint exists
    * 根据点的名称,判断端点是否存在,存在返回true
   */
  def verify(name: String): Boolean = {
    endpoints.containsKey(name)
  }

  /** 用于调度消息的线程池。 */
  private val threadpool: ThreadPoolExecutor = {
    // 可用内内核数：numUsableCores（分配给进程的CPU内核数）不为0使用该值，否则，使用当前JVM可用的最大处理器数
    val availableCores =
      if (numUsableCores > 0) numUsableCores else Runtime.getRuntime.availableProcessors()
    //线程数：获取配置的dispatcher线程数，没有则返回，可用内内核数 or 2 中最大的数
    val numThreads = nettyEnv.conf.getInt("spark.rpc.netty.dispatcher.numThreads",
      math.max(2, availableCores))
    //固定线程数的守护线程
    val pool = ThreadUtils.newDaemonFixedThreadPool(numThreads, "dispatcher-event-loop")
    //为线程池中的每个线程其他任务————MessageLoop（消息循环器）
    for (i <- 0 until numThreads) {
      pool.execute(new MessageLoop)
    }
    // 返回线程池对象
    pool
  }

  /** Message loop used for dispatching messages.
    * 一个线程————用于分发消息的消息循环。
    * */
  private class MessageLoop extends Runnable {
    override def run(): Unit = {
      try {
        while (true) {
          try {
            //从“接受者容器”（阻塞队列）中获取一个端点数据——EndpointData
            val data = receivers.take()
            //如果 端点数据 是“毒药”
            if (data == PoisonPill) {
              // Put PoisonPill back so that other MessageLoops can see it.
              //将“毒药”放回去，以便其他MessageLoops可以看到它。
              receivers.offer(PoisonPill)
              //推出循环
              return
            }
            //处理inbox中存储的消息，及处理message中的消息。
            data.inbox.process(Dispatcher.this)
          } catch {
            case NonFatal(e) => logError(e.getMessage, e)
          }
        }
      } catch {
        case _: InterruptedException => // exit
        case t: Throwable =>
          try {
            // Re-submit a MessageLoop so that Dispatcher will still work if
            // UncaughtExceptionHandler decides to not kill JVM.
            //重新提交一个MessageLoop，保证在之前未捕获的异常发生后 且  不终止JVM的情况下Dispatcher仍然可以工作。
            threadpool.execute(new MessageLoop)
          } finally {
            throw t
          }
      }
    }
  }

  /** A poison endpoint that indicates MessageLoop should exit its message loop.
    * 一个作为毒药存在的端点数据，消息循环遇到该端点，即触发退出逻辑。
    * */
  private val PoisonPill = new EndpointData(null, null, null)
}


/*
* @Description: 一个调用流程
*   1、TransportRequestHandler.processRpcRequest()
*   2、NettyRpcEnv.receive(TransportClient client,ByteBuffer message,RpcResponseCallback callback)
*   3、dispatcher.postRemoteMessage(message: RequestMessage, callback: RpcResponseCallback)
*   4、dispatcher.postMessage(endpointName: String,message: InboxMessage,callbackIfStopped: (Exception) => Unit)
*   5、Inbox.process(dispatcher: Dispatcher)
*       分支5.1 ：....
*             异常：context.sendFailure(e)
*                   -->NettyRpcCallContext.sendFailure(e: Throwable)
*                     --> RemoteNettyRpcCallContext.send(message: Any)
*                        -->TransportRequestHandler.processRpcRequest.RpcResponseCallback.onSuccess(nettyEnv.serialize(message))
*                          --> netyy的channel.writeAndFlush(RpcResponse)
*
*/