package com.yxyl.raft.rpc

import com.yxyl.raft.rpc.entity.*
import io.vertx.core.Future
import io.vertx.core.net.SocketAddress
import io.vertx.kotlin.coroutines.await

interface RaftRpc {

    /**
     * 处理投票请求，当一个raft节点系那个成为一个leader的时候，他会向其他节点发起投票请求
     */
    suspend fun requestVoteSuspend(remote: SocketAddress, requestVote: RequestVote): RequestVoteReply

    fun requestVote(remote: SocketAddress, requestVote: RequestVote): Future<RequestVoteReply>

    /**
     * 处理附加日志请求，leader会使用踏来cp日志条目到其他的节点，并用来提供heartBeat
     */
    suspend fun appendRequestSuspend(remote: SocketAddress, appendRequest: AppendRequest) =
        appendRequest(remote, appendRequest).await()

    fun appendRequest(remote: SocketAddress, appendable: AppendRequest): Future<AppendReply>

    /**
     * 测试连接是否正常
     */
    fun test(remote: SocketAddress): Future<Unit>

    suspend fun testSuspend(remote: SocketAddress) = test(remote).await()

    /**
     * 用来添加新的服务器到集群中
     */
    fun addServer(remote: SocketAddress, request: AddServerRequest): Future<AddServerResponse>


}