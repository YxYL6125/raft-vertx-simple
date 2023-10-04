package com.yxyl.server

import com.yxyl.config.Configuration
import com.yxyl.raft.Raft
import com.yxyl.raft.base.util.countEventLoop
import io.vertx.core.DeploymentOptions
import io.vertx.core.Vertx

/**
 * @program: raft-vertx-simple
 * @description:
 * @author: YxYL
 * @create: 2023-10-04 15:05
 **/

class RaftServer(val configuration: Configuration) {

    val raft: Raft
    val raftServerVerticleFactory: () -> RaftServerVerticle

    init {
        val raftOption = configuration.raftVertxOptions
        raftOption.eventLoopPoolSize = 1
        val raftVertx = Vertx.vertx(raftOption)
        raft = Raft(
            raftVertx,
            configuration.initPeer,
            configuration.raftPort,
            configuration.me,
            configuration.connectNode,
            configuration.httpPort
        )
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