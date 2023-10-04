package com.yxyl.raft.base.raft

import com.yxyl.raft.base.ServerId

data class RaftSnap(
    val nextIndex: Map<ServerId, Int>,
    val matchIndex: Map<ServerId, Int>,
    val peers: Map<ServerId, RaftAddress>,
    val raftState: RaftState
)