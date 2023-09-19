package com.yxyl.raft.base.raft

enum class RaftStatus {
    follower,
    candidate,
    leader
}