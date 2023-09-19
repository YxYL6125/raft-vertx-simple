package com.yxyl.raft.client

import com.yxyl.raft.base.COMMAND_PATH
import com.yxyl.raft.base.PEEK_PATH
import com.yxyl.raft.base.kv.DelCommand
import com.yxyl.raft.base.kv.ReadCommand
import com.yxyl.raft.base.kv.SetCommand
import com.yxyl.raft.base.kv.SimpleKvStateMachineCodec
import com.yxyl.raft.base.raft.RaftSnap
import io.netty.util.internal.EmptyArrays
import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer
import io.vertx.core.net.SocketAddress
import io.vertx.ext.web.client.WebClient
import io.vertx.ext.web.codec.BodyCodec

class RaftClient(val vertx: Vertx, val address: SocketAddress) {
    val webClient = WebClient.create(vertx)

    fun peekRaft() = webClient
        .get(address.port(), address.host(), PEEK_PATH)
        .`as`(BodyCodec.json(RaftSnap::class.java))
        .send()
        .map { it.body() }

    fun del(key: ByteArray) = webClient
        .post(address.port(), address.host(), COMMAND_PATH)
        .`as`(BodyCodec.buffer())
        .sendBuffer(DelCommand.create(key).toBuffer())
        .map { it.body() }

    fun set(key: ByteArray, value: ByteArray) = webClient
        .post(address.port(), address.host(), COMMAND_PATH)
        .`as`(BodyCodec.buffer())
        .sendBuffer(SetCommand.create(key, value).toBuffer())
        .map { it.body() }

    fun get(key: ByteArray) = webClient
        .get(address.port(), address.host(), COMMAND_PATH)
        .`as`(BodyCodec.buffer())
        .sendBuffer(ReadCommand.create(key).toBuffer())
        .map {
            val hasError = it.statusCode() == 500
            var body = it.body() ?: Buffer.buffer()
            if (hasError) {
                DataResult(hasError, body, body.toString())
            } else {
                DataResult(hasError, body)
            }
        }

    fun getAll() = webClient
        .post(address.port(), address.host(), COMMAND_PATH)
        .`as`(BodyCodec.buffer())
        .sendBuffer(ReadCommand.create(EmptyArrays.EMPTY_BYTES).toBuffer())
        .map {
            val hasError = it.statusCode() == 500
            val body = SimpleKvStateMachineCodec.decode(it.body() ?: Buffer.buffer())
            if (hasError) {
                DataResult(hasError, body, body.toString())
            } else {
                DataResult(hasError, body)
            }
        }


}