package com.yxyl.raft

import com.yxyl.exception.NotLeaderException
import com.yxyl.kv.KVStateMachine
import com.yxyl.raft.base.ServerId
import com.yxyl.raft.base.annotation.NonBlocking
import com.yxyl.raft.base.annotation.SwitchThread
import com.yxyl.raft.base.kv.Command
import com.yxyl.raft.base.kv.NoopCommand
import com.yxyl.raft.base.kv.SimpleKvStateMachineCodec
import com.yxyl.raft.base.raft.RaftAddress
import com.yxyl.raft.base.raft.RaftSnap
import com.yxyl.raft.base.raft.RaftState
import com.yxyl.raft.base.raft.RaftStatus
import com.yxyl.raft.base.utils.CountDownLatch
import com.yxyl.raft.base.utils.IntAdder
import com.yxyl.raft.base.utils.wrap
import com.yxyl.raft.rpc.Log
import com.yxyl.raft.rpc.RaftRpc
import com.yxyl.raft.rpc.RaftRpcHandler
import com.yxyl.raft.rpc.RaftRpcImpl
import com.yxyl.raft.rpc.entity.AddServerRequest
import com.yxyl.raft.rpc.entity.AppendReply
import com.yxyl.raft.rpc.entity.AppendRequest
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer
import io.vertx.core.impl.future.PromiseInternal
import io.vertx.kotlin.coroutines.await
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption
import java.time.LocalDateTime
import kotlin.coroutines.CoroutineContext
import kotlin.io.path.Path
import kotlin.random.Random
import kotlin.system.exitProcess


/**
 * Raft的核心类设计 包含论文要求的全部数据 + 工业时间袭来的额外字段，比如说commitIndex之类的
 * 要求与RPC运行在单线程之中，以保护多个属性修改的时候的原子性
 */
class Raft(
    private val singleThreadVertx: Vertx,
    peer: Map<ServerId, RaftAddress>,
    val raftPort: Int,
    val me: ServerId,
    val addModeConfig: RaftAddress? = null,
    val httpPost: Int = -1,
) : AbstractVerticle() {

    companion object {
        const val ElectronInterval = 300
        const val heartBeat = 100L
    }

    //是否能打印
    var enablePrintInfo = true

    //元信息路径
    private val metaInfoPath = Path("raft-$me-meta")

    //元信息
    val metaInfo = FileChannel.open(
        //创建了一个执行特定文件的内存映射
        metaInfoPath,
        StandardOpenOption.CREATE,
        StandardOpenOption.WRITE,
        StandardOpenOption.READ,
    ).map(FileChannel.MapMode.READ_WRITE, 0, 4 + 4 + 1 + Byte.MAX_VALUE.toLong())

    var currentTerm = 0 // 初始化
        set(value) {
            field = value
            metaInfo.putInt(0, value) //把新的值放到内存映射中
        }
        get() = metaInfo.getInt(0)

    //选举投票信息
    //用于在网络服务器中处理，允许我们跟踪哪个服务器(node)已经开始头偏，并将这些信息存储在元数据中。
    //当我们需要检索这些信息的时候，可以轻松地从元数据中获取它们
    var votedFor: ServerId? = null
        set(value) {
            field = value
            if (value == null) metaInfo.put(8, 0)
            else {
                val array = value.toByteArray()
                metaInfo.put(8, array.size.toByte())
                metaInfo.put(9, array)
            }
        }
        get() {
            val length = metaInfo.get(0)
            return if (length == 0.toByte())
                null
            else {
                val array = ByteArray(length.toInt())
                metaInfo.get(9, array)
                String(array)
            }
        }

    var commitIndex = 0
        set(value) {
            field = value
            metaInfo.putInt(4, value)
        }
        get() = metaInfo.getInt(4)

    var lastApplied = 0
    var lastHearBeat = 0L
    var status = RaftStatus.follower
    var leadId: ServerId? = null

    //leader
    //要确保这三个视图一致
    // 1、nextIndex  用来确认发送给follower的日志的下标
    // 2、matchIndex 用来给Leader计算出commitIndex的
    var nextIndexes = mutableMapOf<ServerId, IntAdder>()
    var matchIndexes = mutableMapOf<ServerId, IntAdder>()

    var peers = mutableMapOf<ServerId, RaftAddress>().apply { putAll(peer) }

    private val rpc: RaftRpc
    private val rpcHandler: RaftRpcHandler

    val stateMachine = KVStateMachine(singleThreadVertx, this)

    init {
        if (me.toByteArray().size > Byte.MAX_VALUE) {
            throw IllegalArgumentException("raft-me长度超过Byte.MAX_VALUE")
        }
        RaftRpcImpl(singleThreadVertx, this)
            .also { rpcImpl ->
                rpc = rpcImpl
                rpcHandler = rpcImpl
            }
    }


    override fun start(startPromise: Promise<Void>) {
        /**
         * 预先触发页中断：
         * 它允许操作系统在需要访问尚未加载到内存的页面时提前进行页面加载，以避免延迟和性能下降。
         * 当程序访问一个尚未加载到内存的页面时，通常会触发一个页错误（Page Fault），这时操作系统会将相应的页面从磁盘加载到内存中，并将控制返回给程序继续执行。然而，这个页面加载的过程需要一定的时间，可能会导致程序的执行出现延迟。
         * 为了减少这种延迟，操作系统可以采用预先触发页中断的策略。在这种策略下，操作系统会在程序实际需要访问页面之前，提前将其加载到内存中。这样，当程序真正需要访问这个页面时，页面已经在内存中，就不会触发页错误，从而提高了访问速度和系统的响应性能。
         * 预先触发页中断可以通过各种方式实现，例如预读（预先读取）页面、预先加载相关页面、使用预取算法等。
         * 这种技术在对内存访问较为频繁且延迟敏感的应用中特别有用，可以显著减少页面访问延迟，提高系统的整体性能。
         */
        raftLog("node recover")
        CoroutineScope(context.dispatcher()).launch {
            try {
                if (addModeConfig != null) {
                    //日志
                    raftLog("start in addServerMode")
                    //获取目标node地址
                    var targetAddress = addModeConfig.SocketAddress()
                    //添加node到集群
                    var response = rpc.addServer(
                        targetAddress,
                        AddServerRequest(RaftAddress(raftPort, "localhost", httpPost), me)
                    ).await()

                    /**
                     * while确保新的服务器node被成功添加到集群中，在这个过程中，可能会遇到一些问题：
                     * 比如 leader node 可能在添加过程中发生变化。所以直到 false == response.ok 才停止添加node
                     * 用于处理分布式系统中的leader选举和故障转移的问题
                     */
                    while (response.ok) {
                        targetAddress = response.leader.SocketAddress()
                        response = rpc.addServer(
                            targetAddress,
                            AddServerRequest(RaftAddress(raftPort, "localhost", httpPost), me)
                        ).await()
                    }
                    raftLog("get now leader info $response")
                    leadId = response.leaderId
                    peers.putAll(response.peer)
                    peers[response.leaderId] = RaftAddress(targetAddress).apply { httpPort = response.leader.httpPort }
                }
            } catch (t: Throwable) {
                raftLog("addServerMode start error")
                startPromise.fail(t)
                exitProcess(1)
            }


            stateMachine.init()
                .compose { rpcHandler.init(singleThreadVertx, raftPort) }
                .onSuccess { startTimeCheck() }
                .onSuccess { startPromise.complete() }
                .onFailure(startPromise::fail)
        }


    }

    private fun startTimeCheck() {
        // as => 为了在特定的线程(线程池)上启动新的协程
        CoroutineScope(context.dispatcher() as CoroutineContext).launch {
            val timeout = (ElectronInterval + Random.nextInt(150)).toLong()
            val start = System.currentTimeMillis()
            delay(timeout)
            if (lastHearBeat < start && status != RaftStatus.leader) {
                //todo  startElection()
            }
        }
    }

    /**
     * 将leader的log复制到其他node上去
     */
    private fun broadcastLog(): MutableList<Future<AppendReply>> {
        //表示了向其他的node发送log并等待响应操作
        val list = mutableListOf<Future<AppendReply>>()
        if (peers.isEmpty()) {
            val oldCommitIndex = commitIndex
            calCommitIndex()
            if (oldCommitIndex != commitIndex) {
                stateMachine.applyLog(commitIndex)
            }
            return list
        }
        for (peer in peers) {
            val peerServerId = peer.key
            val nextIndex = nextIndexes[peerServerId] ?: continue
            list.add(appendLogsToPeer(nextIndex, peer, peerServerId))
        }
        return list
    }

    private fun appendLogsToPeer(
        nextIndex: IntAdder,
        peer: MutableMap.MutableEntry<ServerId, RaftAddress>,
        peerServerId: ServerId,
    ): Future<AppendReply> {
        val nextIndexValue = nextIndex.value
        val logIndexSnap = getNowLogIndex()
        val ar = if (nextIndexValue > logIndexSnap) {
            AppendRequest(
                currentTerm,
                logIndexSnap,
                stateMachine.getLastLogTerm(),
                commitIndex,
                listOf()
            )
        } else {
            //next rf 0 -> 1
            //从哪边开始就是左边界
            val slice = stateMachine.sliceLogs(nextIndexValue)
            val term = getTermByIndex(nextIndexValue - 1)
            AppendRequest(
                currentTerm,
                nextIndexValue - 1,
                term,
                commitIndex,
                slice
            )
        }
        return rpc.appendRequest(peer.value.SocketAddress(), ar).onSuccess {
            if (it.isSuccess) {
                //更新匹配索引 & 下一个索引
                matchIndexes[peerServerId]?.value = logIndexSnap
                nextIndexes[peerServerId]?.value = logIndexSnap - 1
                val oldCommitIndex = commitIndex
                //重新计算commitIndex
                calCommitIndex()
                if (oldCommitIndex != commitIndex) {
                    //应用log到交换机器
                    stateMachine.applyLog(commitIndex)
                }
            } else {
                if (it.term > currentTerm) {
                    becomeFollower(it.term)
                } else {
                    adjustNextIndex(peerServerId, it)
                }
            }
        }
    }

    private fun adjustNextIndex(serverId: ServerId, _reply: AppendReply) {
        // TODO: 待完善
        nextIndexes[serverId]?.add(-1)
    }

    /**
     * 化身follower
     */
    internal fun becomeFollower(term: Int) {
        status = RaftStatus.follower
        currentTerm = term
        votedFor = null
        lastHearBeat = System.currentTimeMillis()
    }

    /**
     * 化身candidate
     */
    fun becomeCandidate() {
        status = RaftStatus.candidate
        commitIndex++
        votedFor = me
        lastHearBeat = System.currentTimeMillis()

    }

    /**
     * 化身leader
     */
    fun becomeLeader() {
        status = RaftStatus.leader
        leadId = me
        lastHearBeat = System.currentTimeMillis()
        nextIndexes = mutableMapOf()
        matchIndexes = mutableMapOf()
        peers.forEach {
            nextIndexes[it.key] = IntAdder(stateMachine.getNowLogIndex() + 1)
            matchIndexes[it.key] = IntAdder(0)
        }
        //添加一个空日志[论文需要]
        addLog(NoopCommand())
        //不断心跳
        CoroutineScope(context.dispatcher() as CoroutineContext).launch {
            //不断的向所有Follower广播日志，并在每次广播之间暂停一段时间(HeartBeat)
            while (status == RaftStatus.leader) {
                broadcastLog()
                delay(heartBeat)
            }
        }
    }

    fun getTermByIndex(index: Int) = stateMachine.getTermByIndex(index)

    fun getNowLogIndex(): Int = stateMachine.getNowLogIndex()

    fun getLastLogTerm(): Int = stateMachine.getLastLogTerm()

    fun insertLogs(prevLogIndex: Int, logs: List<Log>) = stateMachine.insertLogs(prevLogIndex, logs)


    /**
     * 计算提交索引
     * commitIndex:是值得已经被复制到大多数服务器并且可以安全带应用到状态机的log的索引
     */
    private fun calCommitIndex() {
        val values = matchIndexes.values.toMutableList()
        values.add(IntAdder(getNowLogIndex()))
        val list = values.sortedBy(IntAdder::value)
        val index = if (list.size % 2 == 0) {
            list.size / 2 - 1
        } else {
            list.size / 2
        }
        commitIndex = list[index].value
    }


    /**
     * 会调跑在Raft实例绑定的EventLoop上
     */
    @NonBlocking
    @SwitchThread(Raft::class)
    fun startRaft(): Future<Unit> = singleThreadVertx.deployVerticle(this)
        .map {}


    fun raftLog(msg: String) {
        if (!enablePrintInfo) {
            return
        }
        println("[${LocalDateTime.now()} serverId:$me term:$currentTerm index = ${getNowLogIndex()} status:${status} voteFor: ${votedFor}]: message:$msg")
    }


    /**
     * 外部调用的一个接口，所以要保证线程安全
     */
    @NonBlocking
    @SwitchThread(Raft::class)
    fun addLog(command: Command, promise: Promise<Unit>) {
        if (leadId != me) {
            promise.fail(
                NotLeaderException("not leader!", peers[leadId])
            )
            return
        }
        stateMachine.addLog(command, promise::complete)
    }

    @NonBlocking
    @SwitchThread(Raft::class)
    fun addLog(command: Command) {
        val promise = Promise.promise<Unit>()
        addLog(command, promise)
    }


    @NonBlocking
    @SwitchThread(Raft::class)
    fun lineRead(key: ByteArray, promise: PromiseInternal<Buffer>) {
        //leader检查
        if (leadId != me) {
            promise.fail(
                NotLeaderException("not leader!", peers[leadId])
            )
            return
        }
        //日志广播
        context.runOnContext {
            val readIndex = commitIndex
            val waiters = broadcastLog()

            //等待确认，等直到一半的node确认接收到了log，(使用计时器来实现)
            val downLatch = CountDownLatch(waiters.size / 2)
            waiters.forEach { f ->
                f.onComplete { downLatch.countDown() }
            }

            CoroutineScope(context.dispatcher() as CoroutineContext).launch {
                //开始读取操作
                downLatch.wait()
                if (status != RaftStatus.leader) {
                    promise.fail(
                        NotLeaderException("not leader", peers[leadId])
                    )
                    return@launch
                }
                val readAction = if (key.isEmpty()) {
                    { promise.complete(SimpleKvStateMachineCodec.encode(stateMachine.db)) }
                } else {
                    { promise.complete(wrap(stateMachine.getDirect(key))) }
                }

                //应用日志条目：如果commitLogIndex > lastApplied，就吧读取操作添加到状态机的事件队列里面；反之立即执行读取操作
                if (readIndex <= lastApplied) {
                    readAction()
                } else {
                    stateMachine.applyEventWaitQueue.offer(readIndex to readAction)
                }
            }
        }
        return
    }

    @SwitchThread(Raft::class)
    fun raftSnap(fn: (RaftSnap) -> Unit) {
        context.runOnContext {
            val currentState =
                RaftState(currentTerm, votedFor, status, commitIndex, lastApplied, leadId)
            val raftSnap = RaftSnap(
                nextIndexes.mapValues { it.value.value },
                matchIndexes.mapValues { it.value.value },
                peers.toMap(),
                currentState
            )
            fn(raftSnap)
        }
    }
}