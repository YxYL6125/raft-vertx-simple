package com.yxyl.exception;


import com.yxyl.raft.base.raft.RaftAddress;
import io.vertx.core.impl.NoStackTraceThrowable;

public class NotLeaderException extends NoStackTraceThrowable {

    public final RaftAddress leaderInfo;

    public NotLeaderException(String message, RaftAddress leaderInfo) {
        super(message);
        this.leaderInfo = leaderInfo;
    }
}
