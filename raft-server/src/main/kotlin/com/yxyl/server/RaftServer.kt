package com.yxyl.server

import com.yxyl.configuration.Configuration
import com.yxyl.raft.Raft
import com.yxyl.raft.base.utils.countEventLoop
import io.vertx.core.DeploymentOptions
import io.vertx.core.Vertx

class RaftServer(val configuration: Configuration) {
    val raft: Raft
    val raftServerVerticleFactory: () -> RaftServerVerticle

    init {
        val raftOptions = configuration.raftVertxOptions
        raftOptions.eventLoopPoolSize = 1
        val raftVertx = Vertx.vertx(raftOptions)
        raft = Raft(
            raftVertx,
            configuration.initPeer,
            configuration.raftPort,
            configuration.me,
            configuration.connectNode,
            configuration.httpPort
        )
        //工厂模式
        raftServerVerticleFactory = { RaftServerVerticle(configuration, raft) }
    }

    fun start() = configuration.httpVertx.let { serverVertx ->
        raft.startRaft()
            .flatMap {
                serverVertx.deployVerticle(
                    raftServerVerticleFactory,
                    DeploymentOptions().setInstances(serverVertx.countEventLoop())
                )
            }
            .onFailure {
                raft.raftLog("start error! error message:${it.message}")
            }
            .map { }
    }
}