package com.yxyl.raft.base.annotation

import kotlin.reflect.KClass

/**
 * 标识一个方法内部并不在当前线程中执行
 * 只有个标识的作用
 */
@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.SOURCE)
annotation class SwitchThread(val thread: KClass<*>)
