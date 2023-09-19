package com.yxyl.raft.base.kv

import io.vertx.core.buffer.Buffer

/**
 * 为键值状态机快照提供编码和解码的功能
 */
class SimpleKvStateMachineCodec {

    companion object {
        fun encode(stateMachineSnap: Map<ByteArrayKey, ByteArray>): Buffer {
            var buffer = Buffer.buffer()
            stateMachineSnap.forEach { k, v ->
                buffer = buffer
                    .appendInt(k.byteArray.size + v.size)
                    .appendInt(k.size())
                    .appendBytes(k.byteArray)
                    .appendBytes(v)
            }
            return buffer
        }


        fun decode(buffer: Buffer): Map<ByteArrayKey, ByteArray> {
            var hasRead = 0;
            val res = mutableMapOf<ByteArrayKey, ByteArray>()
            while (hasRead < buffer.length()) {
                val totalLength = buffer.getInt(hasRead)
                val keyLength = buffer.getInt(hasRead + 4)
                val valueLength = totalLength - keyLength
                hasRead += 8
                val key = buffer.getBytes(hasRead, hasRead + keyLength)
                hasRead += keyLength
                val value = buffer.getBytes(hasRead, hasRead + valueLength)
                res[ByteArrayKey(key)] = value
                hasRead += valueLength
            }
            return res
        }
    }


}