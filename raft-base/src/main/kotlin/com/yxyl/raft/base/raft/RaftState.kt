package com.yxyl.raft.base.raft

import com.yxyl.raft.base.ServerId

data class RaftState(
    val currentTerm: Int,
    val voteFor: ServerId?,
    val status: RaftStatus,
    val commitIndex: Int,
    val applied: Int,
    val leader: ServerId?
)