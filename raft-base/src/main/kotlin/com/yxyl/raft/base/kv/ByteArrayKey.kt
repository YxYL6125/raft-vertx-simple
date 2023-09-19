package com.yxyl.raft.base.kv

class ByteArrayKey(val byteArray: ByteArray) {
    fun size() = byteArray.size
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as ByteArrayKey

        return byteArray.contentEquals(other.byteArray)
    }

    override fun hashCode(): Int {
        return byteArray.contentHashCode()
    }


}