package com.yxyl.configuration

import com.yxyl.raft.base.RandomServerId
import com.yxyl.raft.base.ServerId
import com.yxyl.raft.base.raft.RaftAddress
import com.yxyl.raft.base.utils.singleVertxConfig
import io.vertx.core.Vertx
import io.vertx.core.VertxOptions
import io.vertx.core.json.JsonObject

class Configuration(
    val me: ServerId = RandomServerId(),
    val raftPort: Int = 8080,
    val httpPort: Int = 80,
    httpVertxOptions: Map<String, Any> = mutableMapOf(),
    initPeerArgs: Map<ServerId, RaftAddress> = mapOf(),
    val connectNode: RaftAddress? = null,
) {
    val initPeer = initPeerArgs
    val raftVertxOptions = singleVertxConfig()
    val httpVertx = Vertx.vertx(VertxOptions(JsonObject(httpVertxOptions)))
    override fun toString(): String {
        return "Configuration(me='$me', raftPort=$raftPort, httpPost=$httpPort, connectNode=$connectNode, initPeer=$initPeer, raftVertxOptions=$raftVertxOptions, httpVertx=$httpVertx)"
    }

}