package com.yxyl.raft.base.annotation

/**
 * 标识这个方法不会阻塞当前线程
 * 只有个标识的作用
 */
@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.SOURCE)
annotation class NonBlocking()
