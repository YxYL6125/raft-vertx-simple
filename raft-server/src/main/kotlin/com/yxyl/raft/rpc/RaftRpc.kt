package com.yxyl.raft.rpc

import com.yxyl.raft.rpc.entity.*
import io.vertx.core.Future
import io.vertx.core.net.SocketAddress

/**
 * @program: raft-vertx-simple
 * @description:
 * @author: YxYL
 * @create: 2023-10-04 15:34
 **/

interface RaftRpc {
    
    fun requestVote(remote: SocketAddress, requestVote: RequestVote): Future<RequestVoteReply>
    
    suspend fun requestVoteSuspend(remote: SocketAddress, requestVote: RequestVote): RequestVoteReply
    
    fun appendRequest(remote: SocketAddress, appendRequest: AppendRequest): Future<AppendReply>
    
    fun test(remote: SocketAddress): Future<Unit>

    fun addServer(remote: SocketAddress, request: AddServerRequest): Future<AdderServerResponse>
    
}