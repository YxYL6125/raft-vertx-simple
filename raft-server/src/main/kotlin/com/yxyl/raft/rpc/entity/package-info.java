package com.yxyl.raft.rpc.entity;
/**
 * 当一个node想成为leader时候，它会创建一个 RequestVote对象，并发给其他node,这个请求包含了candidateId,term……
 * 当其他node受到这个请求后，他们会根据自己的stata来决定是否给该node投票，然后他们会创建一个RequestVoteReply对象来标志他们的投票决定
 */