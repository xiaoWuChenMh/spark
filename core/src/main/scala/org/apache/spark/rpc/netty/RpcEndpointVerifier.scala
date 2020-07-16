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

import org.apache.spark.rpc.{RpcCallContext, RpcEndpoint, RpcEnv}

/**
 * An [[RpcEndpoint]] for remote [[RpcEnv]]s to query if an `RpcEndpoint` exists.
 * 一个用于远程[[RpcEnv]]的[[RpcEndpoint]]，以查询是否存在`RpcEndpoint`。
 * This is used when setting up a remote endpoint reference.
 * 在设置远程端点引用时使用。
 */
private[netty] class RpcEndpointVerifier(override val rpcEnv: RpcEnv, dispatcher: Dispatcher)
  extends RpcEndpoint {

  //偏函数：接收信息 并 回复
  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    //类型匹配，判断给定的名称所对应的端点是否存在，把结果通过 context.reply（内部是netty）把结果返回到远程，其中一个调用链如下：
      //NettyRpcCallContext.reply
      //RemoteNettyRpcCallContext.send
      //TransportRequestHandler.processRpcRequest.RpcResponseCallback.onSuccess(nettyEnv.serialize(message))
      //netyy的channel.writeAndFlush(RpcResponse)
    case RpcEndpointVerifier.CheckExistence(name) => context.reply(dispatcher.verify(name))
  }
}

private[netty] object RpcEndpointVerifier {
  val NAME = "endpoint-verifier"

  /** A message used to ask the remote [[RpcEndpointVerifier]] if an `RpcEndpoint` exists.
    * 一条消息，用于询问远程[[RpcEndpointVerifier]]是否存在 `RpcEndpoint`。
    * */
  case class CheckExistence(name: String)
}
