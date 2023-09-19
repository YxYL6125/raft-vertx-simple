package com.yxyl.raft.rpc.entity

import com.yxyl.raft.base.ServerId
import com.yxyl.raft.base.raft.RaftAddress

class AddServerResponse(
    val ok: Boolean,
    val leader: RaftAddress,
    val leaderId: ServerId,
    val peer: Map<ServerId, RaftAddress>,
) {

    override fun toString(): String {
        return "AddServerResponse(ok=$ok, leader=$leader, leaderId='$leaderId', peer=$peer)"
    }
}