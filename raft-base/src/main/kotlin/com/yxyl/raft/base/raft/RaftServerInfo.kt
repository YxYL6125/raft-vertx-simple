package com.yxyl.raft.base.raft

import com.yxyl.raft.base.RandomServerId
import com.yxyl.raft.base.ServerId

data class RaftServerInfo(
    var raftAddress: RaftAddress,
    val serverId: ServerId = RandomServerId(),
) {
    override fun toString(): String {
        return "RaftServerInfo(raftAddress=$raftAddress, serverId='$serverId')"
    }
}