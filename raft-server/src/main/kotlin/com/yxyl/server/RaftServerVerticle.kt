package com.yxyl.server

import com.yxyl.config.Configuration
import com.yxyl.exception.NotLeaderException
import com.yxyl.exception.UnknownCommandException
import com.yxyl.raft.Raft
import com.yxyl.raft.base.*
import com.yxyl.raft.base.kv.Command
import com.yxyl.raft.base.kv.DelCommand
import com.yxyl.raft.base.kv.ReadCommand
import com.yxyl.raft.base.kv.SetCommand
import com.yxyl.raft.base.raft.RaftSnap
import com.yxyl.raft.base.util.EMPTY_BUFFER
import com.yxyl.raft.base.util.suspendHandle
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.buffer.Buffer
import io.vertx.core.impl.ContextInternal
import io.vertx.ext.web.Router
import io.vertx.kotlin.coroutines.await

/**
 * @program: raft-vertx-simple
 * @description:
 * @author: YxYL
 * @create: 2023-10-04 15:16
 **/

class RaftServerVerticle(val configuration: Configuration, val raft: Raft) : AbstractVerticle() {

    private lateinit var internalContext: ContextInternal

    override fun start(startPromise: Promise<Void>) {
        internalContext = context as ContextInternal
        val router = Router.router(vertx)
        router.post(COMMAND_PATH)
            .suspendHandle {
                val body = it.request().body().await()
                try {
                    val response = handleCommandRequest(body).await()
                    it.response().statusCode = SUCCESS
                    it.end(response.result)
                } catch (leader: NotLeaderException) {
                    it.response().statusCode = NOT_LEAD
                    it.json(leader.leaderInfo)
                } catch (t: Throwable) {
                    it.response().statusCode = FAIL
                    it.end(t.message)
                }
            }
        router.get(PEEK_PATH)
            .suspendHandle {
                val snap = peekRaft().await()
                it.json(snap)
            }

        vertx.createHttpServer()
            .requestHandler(router)
            .listen(configuration.httpPort)
            .onComplete {
                if (it.succeeded()) {
                    startPromise.complete()
                } else {
                    startPromise.fail(it.cause())
                }
            }
    }

    private fun peekRaft(): Future<RaftSnap> {
        val promise = internalContext.promise<RaftSnap>()
        raft.raftSnap(promise::complete)
        return promise.future()
    }


    private fun handleCommandRequest(body: Buffer): Future<CommandResponse> {

        val request = CommandRequest.decode(body)
        val res = when (val command = request.command) {
            is DelCommand, is SetCommand -> {
                val promise = internalContext.promise<Unit>()
                raft.addLog(command, promise)
                promise.future()
                    .map { CommandResponse() }
            }

            is ReadCommand -> {
                val promise = internalContext.promise<Buffer>()
                raft.lineRead(command.key, promise)
                promise.future()
                    .map {
                        CommandResponse(it)
                    }
            }

            else -> {
                val promise = internalContext.promise<CommandResponse>()
                promise.fail(UnknownCommandException(command::class.simpleName))
                promise.future()
            }
        }

        return res
    }

    @JvmInline
    value class CommandResponse(val result: Buffer = EMPTY_BUFFER) {
    }

    class CommandRequest(val command: Command) {
        companion object {
            fun decode(body: Buffer): CommandRequest {
                val rawCommand = body.bytes
                return CommandRequest(Command.transToCommand(rawCommand))
            }
        }
    }

}