package com.yxyl.kv

import com.yxyl.raft.Raft
import com.yxyl.raft.base.annotation.NonBlocking
import com.yxyl.raft.base.annotation.SwitchThread
import com.yxyl.raft.base.kv.*
import com.yxyl.raft.base.utils.IntAdder
import com.yxyl.raft.base.utils.removeAll
import com.yxyl.raft.rpc.Log
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.Vertx
import io.vertx.core.buffer.impl.VertxByteBufAllocator
import java.nio.channels.FileChannel
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.util.*
import java.util.concurrent.Executors
import kotlin.collections.ArrayDeque
import kotlin.concurrent.thread


/**
 * 与任raft共享一个线程
 * 内部的file则是另外一个线程，而这个线程[logDispatcher]的任务顺序由raft驱动线程控制
 * 所有raft和statemachine的log视图最终一直
 */
private typealias LazyTask = () -> Unit
private typealias AfterComplete = () -> Unit

class KVStateMachine(private val vertx: Vertx, private val rf: Raft) {

    var logs = mutableListOf<Log>()
    private val logBase = 0
    private val termBase = 0
    private val logDispatcher = LogDispatcher(rf.me)
    private val serverChangeWaitQueue = ArrayDeque<LazyTask>()


    val db = mutableMapOf<ByteArrayKey, ByteArray>()

    /**
     * 等待需要被apply的index、key、value回调
     */
    val applyEventWaitQueue =
        PriorityQueue<Pair<Int, () -> Unit>>(Comparator.comparing { it.first })

    /**
     * 在 [Raft.startRaft]中调用
     * 所以其中跑在EventLoop中
     */
    fun init(): Future<Unit> {
        val promise = Promise.promise<Unit>()
        vertx.setTimer(5000) { logDispatcher.sync() }
        logDispatcher.recoverLog {
            logs = it
            vertx.runOnContext { promise.complete() }
        }
        return promise.future()
    }


    private fun handleServerConfigChangeCommand(
        command: ServerConfigChangeCommand,
        callback: () -> Unit,
        currentIndex: Int,
    ) {
        //如果命令中的服务器ID与当前服务器的ID相同，那么函数将直接返回
        if (command.serverInfo.serverId == rf.me) {
            return
        }

        //更新服务器配置
        //将回掉放在队列里面去
        serverChangeWaitQueue.add {
            val serverId = command.serverInfo.serverId
            rf.peers[serverId] = command.serverInfo.raftAddress
            rf.raftLog("apply server add, new peer is ${command.serverInfo.raftAddress}")
            //设置下一个term
            rf.nextIndexes[serverId] = IntAdder(getNowLogIndex() + 1)
            rf.matchIndexes[serverId] = IntAdder(0)
            applyEventWaitQueue.offer(currentIndex to {
                serverChangeWaitQueue.removeFirst()
                serverChangeWaitQueue.firstOrNull()?.invoke()
            })
            callback()
        }
        if (serverChangeWaitQueue.size == 1) {
            //直接执行
            serverChangeWaitQueue.first().apply { this.invoke() }
        }
    }


    /**
     * endIndex: 右端点(包含)
     */
    fun applyLog(endIndex: Int) {
        val applyIndexInLogs = rf.lastApplied - logBase - 1
        val endIndexInLogs = endIndex - logBase - 1
        for (i in applyIndexInLogs + 1..endIndexInLogs) {
            when (val command = Command.transToCommand(logs[i].command)) {
                is DelCommand -> db.remove(ByteArrayKey(command.key))
                is NoopCommand -> {}
                is SetCommand -> db[ByteArrayKey(command.key)] = command.value
                //不做任何事情以防止被continue直接忽略而不走apply
                is ServerConfigChangeCommand -> {}
                else -> continue
            }
            rf.lastApplied++
            while (!applyEventWaitQueue.isEmpty() && rf.lastApplied >= applyEventWaitQueue.firstOrNull()!!.first) {
                //执行放在队列里边的操作
                val (_, callback) = applyEventWaitQueue.poll()
                callback()
            }
        }
    }

    fun getNowLogIndex(): Int = logs.size + logBase

    fun getLastLogTerm(): Int = if (logs.isEmpty()) termBase else logs.last().term

    fun sliceLogs(startIndex: Int): List<Log> {
        val inLogs = if (startIndex < 1) 0 else startIndex - logBase - 1
        return logs.slice(inLogs until logs.size)
    }

    fun getTermByIndex(index: Int): Int {
        if (index < 1) return 0
        val logIndex = index - logBase - 1
        return logs[index].term
    }

    /**
     * leader状态时client请求附加日志
     */
    @NonBlocking
    @SwitchThread(Raft::class)
    fun addLog(command: Command, callback: () -> Unit) {
        vertx.runOnContext {
            val index = getNowLogIndex() + 1
            //创建一个新的日志条目
            val log = Log(index, rf.currentTerm, command.toByteArray())
            logs.add(log)
            //分发给所有的follower
            logDispatcher.appendLogs(listOf(log))

            if (command !is ServerConfigChangeCommand) {
                //将回调函数添加到等待队列中
                applyEventWaitQueue.offer(index to callback)
            } else {
                //这里特殊直接就apply
                handleServerConfigChangeCommand(command, callback, index)
            }
        }
    }

    /**
     * 作为接受者的日志
     */
    @NonBlocking
    fun insertLogs(prevIndex: Int, logs: List<Log>) {
        if (prevIndex > getNowLogIndex() || prevIndex < logBase) throw IllegalArgumentException("")
        var logNeedRemove = this.logs.removeAll(prevIndex - logBase)
        val decreaseSize = logNeedRemove.sumOf(Log::size)
        //raft实际上是连续的index
        //所以修补日志的时候，对于已经持久化的log可以直接利用内存中的log的删除信息去truncate日志文件 再append就行了
        //由于raft的特性commit的log不会被删除
        if (decreaseSize != 0) logDispatcher.decreaseSize(decreaseSize)
        this.logs.addAll(logs)
        logDispatcher.appendLogs(logs)

        //这里特殊的直接就apply
        logs.forEach {
            if (ServerConfigChangeCommand.isServerConfigChangeCommand(it.command)) {
                handleServerConfigChangeCommand(
                    ServerConfigChangeCommand(it.command), {}, getNowLogIndex()
                )
            }
        }
    }

    fun getDirect(key: ByteArray): ByteArray? = db[ByteArrayKey(key)]


    //todo many func


    inner class LogDispatcher(private val logFileName: String) {
        private val executor = Executors.newSingleThreadExecutor { r ->
            Thread(r, "raft-$logFileName-log-Thread")
        }

        init {
            Runtime.getRuntime().addShutdownHook(
                thread
                    (
                    start = false,
                    name = "LogDispatcher-close"
                ) { this.close() })
        }

        val logFile: FileChannel =
            FileChannel.open(
                Path.of(logFileName),
                StandardOpenOption.APPEND,
                StandardOpenOption.CREATE,
                StandardOpenOption.SYNC
            )

        /**
         * 追加log的操作
         */
        fun appendLog(log: Log, afterWriterToFile: () -> Unit) {
            executor.execute {
                log.writeToFIle(logFile)
                afterWriterToFile()
            }
        }


        //TODO
        @NonBlocking
        @SwitchThread(LogDispatcher::class)
        fun recoverLog(callback: (MutableList<Log>) -> Unit) {
            executor.execute {
                val entries = mutableListOf<Log>()
                val buf = VertxByteBufAllocator.POOLED_ALLOCATOR.buffer()
                val logFile = FileChannel.open(Path.of(logFileName), StandardOpenOption.READ)
                logFile.run {
                    buf.writeBytes(logFile, 0L, logFile.size().toInt())
                    while (buf.readableBytes() != 0) {
                        val index = buf.readInt()
                        val term = buf.readInt()
                        val length = buf.readInt()
                        val command = ByteArray(length)
                        buf.readBytes(command)
                        entries.add(Log(index, term, command))
                    }
                    callback(entries)
                }
            }
        }

        fun execute(command: Runnable) {
            executor.execute(command)
        }

        fun close() {
            logFile.force(true)
            executor.shutdown()
        }

        @NonBlocking
        @SwitchThread(LogDispatcher::class)
        fun decreaseSize(size: Int) {
            executor.execute {
                logFile.truncate(logFile.size() - size)
            }
        }

        /**
         * 不阻塞当前线程
         */
        @NonBlocking
        @SwitchThread(LogDispatcher::class)
        fun sync() {
            executor.execute {
                //todo understand?
                rf.metaInfo.force()
                logFile.force(true)
            }
        }


        @NonBlocking
        @SwitchThread(LogDispatcher::class)
        fun appendLogs(log: List<Log>) {
            executor.execute {
                log.forEach {
                    it.writeToFIle(logFile)
                }
            }
        }


    }
}