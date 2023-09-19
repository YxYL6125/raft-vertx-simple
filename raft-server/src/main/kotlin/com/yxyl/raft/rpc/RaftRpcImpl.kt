package com.yxyl.raft.rpc

import com.yxyl.raft.Raft
import com.yxyl.raft.base.ADD_SERVER_PATH
import com.yxyl.raft.base.ServerId
import com.yxyl.raft.base.raft.RaftAddress
import com.yxyl.raft.base.raft.RaftServerInfo
import com.yxyl.raft.base.raft.RaftStatus
import com.yxyl.raft.base.utils.suspendHandle
import com.yxyl.raft.rpc.entity.*
import io.vertx.core.CompositeFuture
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.Vertx
import io.vertx.core.json.Json
import io.vertx.core.json.JsonObject
import io.vertx.core.net.SocketAddress
import io.vertx.ext.web.Router
import io.vertx.ext.web.client.WebClient
import io.vertx.ext.web.client.WebClientOptions
import io.vertx.ext.web.codec.BodyCodec
import io.vertx.ext.web.handler.BodyHandler
import io.vertx.kotlin.coroutines.await

class RaftRpcImpl(
    private val vertx: Vertx,
    private val raft: Raft,
) : RaftRpc, RaftRpcHandler {

    private val raftRpcClient = WebClient.create(
        vertx,
        WebClientOptions()
            .setMaxPoolSize(1)
            .setIdleTimeout(0)
    )
    private var port = -1

    companion object {
        const val appendRequest_path = "/appendRequest"
        const val requestVoteReply_path = "/requestVote"
        const val test_path = "/test"
        const val server_id_header = "raft_server_id"
    }


    /**
     *      这个方法是在实现Raft协议中的投票请求部分。下面是对这个方法的详细解释：
     *       override fun requestVote：这个函数覆盖了RaftRpc接口中的requestVote方法。它接受两个参数：一个是远程服务器的地址(remote)，另一个是投票请求(requestVote)。
     *       raftRpcClient.post(remote.port(), remote.host(), requestVoteReply_path)：这行代码使用WebClient向远程服务器发送一个POST请求。请求的路径是requestVoteReply_path，这是在伴生对象中定义的一个常量。
     *      .putHeader(server_id_header, raft.me)：这行代码在请求头中添加了一个键值对，键是server_id_header（也是在伴生对象中定义的一个常量），值是当前节点的ID(raft.me)。
     *      .as(BodyCodec.buffer())：这行代码设置了请求体的编码方式为buffer。
     *      .sendBuffer(requestVote.toBuffer())：这行代码将投票请求转换为buffer，然后发送出去。
     *      .map { RequestVoteReply(it.body()) }：这行代码将收到的响应体映射为RequestVoteReply对象。
     *       catch (e: Exception) { Future.failedFuture(e) }：如果在发送请求或处理响应时发生异常，这个方法会返回一个失败的Future。
     */
    override fun requestVote(
        remote: SocketAddress,
        requestVote: RequestVote,
    ): Future<RequestVoteReply> = try {
        raftRpcClient.post(remote.port(), remote.host(), requestVoteReply_path)
            .putHeader(server_id_header, raft.me)
            .`as`(BodyCodec.buffer())
            .sendBuffer(requestVote.toBuffer())
            .map { RequestVoteReply(it.body()) }
    } catch (e: Exception) {
        Future.failedFuture(e)

    }

    override fun test(remote: SocketAddress): Future<Unit> = try {
        raftRpcClient.post(remote.port(), remote.host(), test_path)
            .putHeader(server_id_header, raft.me)
            .`as`(BodyCodec.buffer())
            .send()
            .flatMap {
                if (it.statusCode() != 200) {
                    Future.failedFuture("PING Fail")
                } else {
                    Future.succeededFuture()
                }
            }
    } catch (e: Exception) {
        Future.failedFuture(e)
    }

    override fun appendRequest(
        remote: SocketAddress,
        appendable: AppendRequest,
    ): Future<AppendReply> = try {
        raftRpcClient.post(remote.port(), remote.host(), appendRequest_path)
            .putHeader(server_id_header, raft.me)
            .`as`(BodyCodec.buffer())
            .send()
            .map { AppendReply(it.body()) }
    } catch (e: Exception) {
        Future.failedFuture(e)
    }


    override fun addServer(
        remote: SocketAddress,
        request: AddServerRequest,
    ): Future<AddServerResponse> = try {
        raftRpcClient.post(remote.port(), remote.host(), ADD_SERVER_PATH)
            .putHeader(server_id_header, raft.me)
            .`as`(BodyCodec.json(AddServerResponse::class.java))
            .sendBuffer(Json.encodeToBuffer(request))
            .map { it.body() }
    } catch (e: Exception) {
        Future.failedFuture(e)
    }

    override suspend fun requestVoteSuspend(remote: SocketAddress, requestVote: RequestVote): RequestVoteReply =
        requestVote(remote, requestVote).await()


    override fun init(vertx: Vertx, raftPort: Int): Future<Unit> {
        this.port = raftPort
        val router = Router.router(vertx)
        router.post(appendRequest_path)
            .handler(BodyHandler.create(false))
            .handler {
                val body = it.body().buffer()
                val appendRequest = AppendRequest(body, it.request().getHeader(server_id_header))
                it.end(appendRequest(appendRequest).toBuffer())
            }

        router.post(requestVoteReply_path)
            .handler(BodyHandler.create(false))
            .handler {
                it.end(
                    RequestVote(
                        RequestVote(
                            it.request().getHeader(server_id_header),
                            it.body().buffer()
                        )
                    ).toBuffer()
                )
            }

        router.post(test_path)
            .handler {
                it.response().statusCode = 200
                it.end()
            }

        router.post(ADD_SERVER_PATH)
            .suspendHandle {
                val body = JsonObject(it.request().body().await())
                    .mapTo(RaftServerInfo::class.java)
                raft.raftLog("recv add Server request: $body")
                body.raftAddress =
                    RaftAddress(body.raftAddress.port, it.request().remoteAddress().host(), body.raftAddress.httpPort)
                val apply = Promise.promise<Map<ServerId, RaftAddress>>()


            }

        return vertx.createHttpServer()
            .requestHandler(router)
            .listen(raftPort)
            .flatMap { s ->
                raft.raftLog("raft core is listening on ${s.actualPort()}")
                CompositeFuture.all(raft.peers.map { test(it.value.SocketAddress()) })
            }
            .map(Unit)

    }

    private fun RequestVote(msg: RequestVote): RequestVoteReply {
        raft.lastHearBeat = System.currentTimeMillis()
        raft.raftLog("receive request vote, msg :${msg}")

        if (msg.term < raft.currentTerm) {
            return RequestVoteReply(raft.currentTerm, false)
        }

        val lastLogTerm = raft.getLastLogTerm()
        val lastLogIndex = raft.getNowLogIndex()

        raft.currentTerm = msg.term
        //如果 voteFor 为空或者已经投给他了
        //如果 voteFor 为空或者为 candidateId,并且候选人的日志至少和自己一样新，那么就投票给热爱
        if ((raft.votedFor == null || raft.votedFor == msg.candidateId) && msg.lastLogIndex >= lastLogIndex) {
            if (msg.lastLogTerm == lastLogTerm && msg.lastLogIndex < lastLogIndex) {
                return RequestVoteReply(raft.currentTerm, false)
            }
            raft.votedFor = msg.candidateId
            raft.raftLog("vote to ${msg.candidateId}")
            return RequestVoteReply(raft.currentTerm, true)
        }

        return RequestVoteReply(raft.currentTerm, false)
    }

    private fun appendRequest(msg: AppendRequest): AppendReply {
        raft.lastHearBeat = System.currentTimeMillis()
        //如果发来log的term比我本身的term还要小的话，直接拉到
        if (msg.term < raft.currentTerm) {
            return AppendReply(raft.currentTerm, false)
        }
        //变成follower
        if (raft.status != RaftStatus.follower) {
            raft.becomeFollower(msg.term)
        }
        //产生了新的leader
        if (raft.leadId != msg.leaderId) {
            //我就不投了
            raft.votedFor = null
            //发送msg的server就是我的leader
            raft.leadId = msg.leaderId
        }
        raft.commitIndex = msg.leaderCommit
        raft.currentTerm = msg.term
        val nowIndex = raft.getNowLogIndex()

        //leader比他长
        if (nowIndex > msg.prevLogIndex) {
            return AppendReply(raft.currentTerm, false)
        }

        //拿到我的任期
        val term = raft.getTermByIndex(msg.prevLogIndex)
        raft.raftLog("leader{${msg.leaderId}}:${msg.prevLogIndex} ${msg.prevLogTerm} follower$nowIndex $term log_count:${msg.entries.size}")
        if (term == msg.prevLogTerm) {
            //我接受log
            raft.insertLogs(msg.prevLogIndex, msg.entries)
            raft.stateMachine.applyLog(raft.commitIndex)
            return AppendReply(raft.currentTerm, true)
        } else {
            return AppendReply(raft.currentTerm, false)
        }
    }


}