package com.yxyl.raft.base.utils

class IntAdder(var value: Int) {

    fun add(v: Int) {
        value += v
    }

    override fun toString(): String {
        return "IntAdder(value=$value)"
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as IntAdder

        return value == other.value
    }

    override fun hashCode(): Int {
        return value
    }


}
