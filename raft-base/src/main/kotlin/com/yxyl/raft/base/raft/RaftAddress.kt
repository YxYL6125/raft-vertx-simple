package com.yxyl.raft.base.raft

import io.vertx.core.net.SocketAddress

data class RaftAddress(val port: Int, val host: String, var httpPort: Int = 8080) {
    constructor(address: SocketAddress?) : this(address?.port() ?: -1, address?.host() ?: "")

    /**
     * 表示一个网络地址
     */
    fun SocketAddress(): SocketAddress = SocketAddress.inetSocketAddress(port, host)
    override fun toString(): String {
        return "RaftAddress(port=$port, host='$host', httPort='$httpPort')"
    }

}
