package com.yxyl.raft.rpc.entity;

import com.yxyl.raft.rpc.Log;
import io.netty.buffer.ByteBuf;
import io.vertx.core.buffer.Buffer;

import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

/**
 * 用来处理 附加日志请求的
 * 用于node之间的同步日志条目
 */
public class AppendRequest {
	private final int term;
	private String leaderId;
	private final int prevLogIndex;
	private final int prevLogTerm;
	private final int leaderCommit;
	private final List<Log> entries;

	public AppendRequest(Buffer body, String remote) {
		ByteBuf buf = body.getByteBuf();
		term = buf.readInt();
		leaderId = remote;
		prevLogIndex = buf.readInt();
		prevLogTerm = buf.readInt();
		leaderCommit = buf.readInt();
		entries = new ArrayList<>();
		while (buf.readableBytes() != 0) {
			var index = buf.readInt();
			var term = buf.readInt();
			var length = buf.readInt();
			var command = new byte[length];
			buf.readBytes(command);
			entries.add(new Log(index, term, command));
		}

	}

	public AppendRequest(int term, int prevLogIndex, int prevLogTerm, int leaderCommit, List<Log> entries) {
		this.term = term;
		this.prevLogIndex = prevLogIndex;
		this.prevLogTerm = prevLogTerm;
		this.leaderCommit = leaderCommit;
		this.entries = entries;
	}

	public Buffer toBuffer() {
		Buffer buffer = Buffer.buffer();
		buffer.appendInt(term)
				.appendInt(prevLogIndex)
				.appendInt(prevLogTerm)
				.appendInt(leaderCommit);
		for (Log log : entries) {
			buffer.appendBuffer(log.toBuffer());
		}
		return buffer;
	}

	public int getTerm() {
		return term;
	}

	public String getLeaderId() {
		return leaderId;
	}

	public int getPrevLogIndex() {
		return prevLogIndex;
	}

	public int getPrevLogTerm() {
		return prevLogTerm;
	}

	public int getLeaderCommit() {
		return leaderCommit;
	}

	public List<Log> getEntries() {
		return entries;
	}

	@Override
	public String toString() {
		return new StringJoiner(", ", AppendRequest.class.getSimpleName() + "[", "]")
				.add("term=" + term)
				.add("leaderId=" + leaderId)
				.add("prevLogIndex=" + prevLogIndex)
				.add("prevLogTerm=" + prevLogTerm)
				.add("leaderCommit=" + leaderCommit)
				.toString();
	}
}