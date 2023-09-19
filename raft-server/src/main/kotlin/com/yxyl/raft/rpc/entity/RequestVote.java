package com.yxyl.raft.rpc.entity;

import io.netty.buffer.ByteBuf;
import io.vertx.core.buffer.Buffer;

import java.util.StringJoiner;

/**
 * 处理投票请求
 */
public class RequestVote {
	private final int term;
	private String candidateId;
	private final int lastLogIndex;
	private final int lastLogTerm;

	public RequestVote(String candidateId, Buffer buffer) {
		ByteBuf buf = buffer.getByteBuf();
		this.candidateId = candidateId;
		term = buf.readInt();
		lastLogIndex = buf.readInt();
		lastLogTerm = buf.readInt();

	}


	public RequestVote(int term, int lastLofIndex, int lastLogTerm) {
		this.term = term;
		this.lastLogIndex = lastLofIndex;
		this.lastLogTerm = lastLogTerm;
	}

	/**
	 * 将类的字段值写到新的Buffer对象中，并返回
	 *
	 * @return
	 */
	public Buffer toBuffer() {
		return Buffer.buffer()
				.appendInt(term)
				.appendInt(lastLogIndex)
				.appendInt(lastLogTerm);
	}

	public int getTerm() {
		return term;
	}


	public int getLastLogIndex() {
		return lastLogIndex;
	}

	public int getLastLogTerm() {
		return lastLogTerm;
	}

	public String getCandidateId() {
		return candidateId;
	}

	@Override
	public String toString() {
		return new StringJoiner(", ", RequestVote.class.getSimpleName() + "[", "]")
				.add("term=" + term)
				.add("candidateId='" + candidateId + "'")
				.add("lastLogIndex=" + lastLogIndex)
				.add("lastLogTerm=" + lastLogTerm)
				.toString();
	}
}
