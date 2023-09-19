package com.yxyl.raft.rpc

import io.vertx.core.buffer.Buffer
import java.nio.ByteBuffer
import java.nio.channels.FileChannel

/**
 *  日志条目
 */
data class Log(val index: Int, val term: Int, val command: ByteArray) {
    val size = 12 + command.size

    fun toBuffer() = Buffer.buffer()
        .appendInt(index)
        .appendInt(term)
        .appendInt(command.size)
        .appendBytes(command)

    companion object {
        fun List<Log>.mergeLogs(): ByteBuffer {
            val totalSize = this.sumOf { it.size }
            val byteBuffer = ByteBuffer.allocateDirect(totalSize)
            for (log in this) {
                byteBuffer.putInt(log.index)
                    .putInt(log.term)
                    .putInt(log.command.size)
                    .put(log.command)
            }
            return byteBuffer
        }
    }


    /**
     * 将日志条目的字段值写入到一个文件通道中
     */
    fun writeToFIle(fileChannel: FileChannel) {
        val allocate = ByteBuffer.allocate(4 + 4 + 4 + command.size)
        val buffer = allocate.putInt(index)
            .putInt(term)
            .putInt(command.size)
            .put(command)

        buffer.flip()
        fileChannel.write(allocate)
    }


}