package com.yxyl.raft

import com.yxyl.exception.NotLeaderException
import com.yxyl.kv.KVStateMachine
import com.yxyl.raft.base.ServerId
import com.yxyl.raft.base.kv.Command
import com.yxyl.raft.base.kv.NoopCommand
import com.yxyl.raft.base.kv.ServerConfigChangeCommand
import com.yxyl.raft.base.kv.SimpleKVStateMachineCodec
import com.yxyl.raft.base.raft.RaftAddress
import com.yxyl.raft.base.raft.RaftSnap
import com.yxyl.raft.base.raft.RaftState
import com.yxyl.raft.base.raft.RaftStatus
import com.yxyl.raft.base.util.CountDownLatch
import com.yxyl.raft.base.util.IntAdder
import com.yxyl.raft.base.util.wrap
import com.yxyl.raft.rpc.RaftRpc
import com.yxyl.raft.rpc.RaftRpcHandler
import com.yxyl.raft.rpc.RaftRpcImpl
import com.yxyl.raft.rpc.entity.*
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer
import io.vertx.kotlin.coroutines.await
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import top.dreamlike.base.util.NonBlocking
import top.dreamlike.base.util.SwitchThread
import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption
import java.time.LocalDateTime
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.coroutines.CoroutineContext
import kotlin.io.path.Path
import kotlin.random.Random
import kotlin.system.exitProcess

/**
 * @program: raft-vertx-simple
 * @description:
 * @author: YxYL
 * @create: 2023-10-04 15:08
 **/

class Raft(
    private val singleThreadVertx: Vertx,
    peer: Map<ServerId, RaftAddress>,
    val raftPort: Int,
    val me: ServerId,
    val addModeConfig: RaftAddress? = null,
    val httpPort: Int = -1
) : AbstractVerticle() {

    companion object {
        const val ElectronInterval = 300
        const val heartBeat = 100L
    }

    var enablePrintInfo = true

    private val metaInfoPath = Path("raft-$me-meta")

    val metaInfo = FileChannel.open(
        metaInfoPath,
        StandardOpenOption.CREATE,
        StandardOpenOption.WRITE,
        StandardOpenOption.READ
    ).map(FileChannel.MapMode.READ_WRITE, 0, 4 + 4 + 1 + Byte.MAX_VALUE.toLong())

    var currentTerm: Int = 0
        set(value) {
            field = value
            metaInfo.putInt(0, value)
        }
        get() = metaInfo.getInt(0)

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
            val length = metaInfo.get(8)
            return if (length == 0.toByte())
                null
            else {
                val array = ByteArray(length.toInt())
                metaInfo.get(9, array)
                String(array)
            }
        }

    //方便快速恢复 也持久化
    var commitIndex: Int = 0
        set(value) {
            field = value
            metaInfo.putInt(4, value)
        }
        get() = metaInfo.getInt(4)

    var lastApplied: Int = 0
    var lastHearBeat = 0L
    var status: RaftStatus = RaftStatus.follower
    var leadId: ServerId? = null

    //leader
    //要确保这三个视图一致
    //nextIndex 是用来确认发送给 follower 的日志的下标，
    //matchIndex 是用来给 Leader 计算出 commitIndex 的
    var nextIndexes = mutableMapOf<ServerId, IntAdder>()
    var matchIndexes = mutableMapOf<ServerId, IntAdder>()

    val peers = mutableMapOf<ServerId, RaftAddress>().apply { putAll(peer) }


    //出现了rpc
    private val rpc: RaftRpc
    private val rpcHandler: RaftRpcHandler
    val stateMachine = KVStateMachine(singleThreadVertx, this)

    init {
        if (me.toByteArray().size > Byte.MAX_VALUE) {
            throw IllegalArgumentException("raft-me长度超过Byte.MAX_VALUE")
        }
        //初始化rpc
        val rpcImpl = RaftRpcImpl(singleThreadVertx, this)
        rpc = rpcImpl
        rpcHandler = rpcImpl
    }

    @NonBlocking
    @SwitchThread(Raft::class)
    fun startRaft(): Future<Unit> {
        return singleThreadVertx.deployVerticle(this)
            .map {}
    }


    override fun start(startPromise: Promise<Void>) {
        //预先触发缺页中断
        raftLog("node recover")
        CoroutineScope(context.dispatcher()).launch {
            try {
                if (addModeConfig != null) {
                    raftLog("start in addServerMode")
                    var targetAddress = addModeConfig.SocketAddress()
                    //首先尝试向目标地址发送addServer请求
                    var response = rpc.addServer(
                        targetAddress,
                        AddServerRequest(RaftAddress(raftPort, "localhost", httpPort), me)
                    ).await()
                    while (!response.ok) {
                        //如果收到的响应表示请求失败（可能是因为目标不是领导者）
                        //那么会更新目标地址并重新发送请求，直到请求成功为止
                        targetAddress = response.leader.SocketAddress()
                        response = rpc.addServer(
                            targetAddress,
                            AddServerRequest(RaftAddress(raftPort, "localhost", httpPort), me)
                        ).await()
                    }
                    raftLog("get now leader info $response")
                    //成功后，更新领导者ID和对等node信息
                    leadId = response.leaderId
                    peers.putAll(response.peer)
                    peers[response.leaderId] = RaftAddress(targetAddress).apply { httpPort = response.leader.httpPort }
                }
            } catch (t: Throwable) {
                raftLog("addServerMode start error")
                startPromise.fail(t)
                exitProcess(1)
            }
            //初始化状态机和RPC处理器，并开始检查超时。如果发生错误，启动Promise失败并退出进程。
            stateMachine.init()
                .compose { rpcHandler.init(singleThreadVertx, raftPort) }
                .onSuccess { startTimeoutCheck() }
                .onSuccess { startPromise.complete() }
                .onFailure(startPromise::fail)
        }
    }

    //超时过程
    private fun startTimeoutCheck() {
        CoroutineScope(context.dispatcher() as CoroutineContext).launch {
            while (true) {
                //超时时间设置为300-450之间
                val timeout = (ElectronInterval + Random.nextInt(150)).toLong()
                val start = System.currentTimeMillis()
                delay(timeout)
                if (lastHearBeat < start && status != RaftStatus.leader) {
                    //开始超时选举
                    startElection()
                }
            }
        }
    }


    private fun startElection() {
        becomeCandidate()
        raftLog("start election")
        val buffer = RequestVote(currentTerm, stateMachine.getNowLogIndex(), stateMachine.getLastLogTerm())
        //法定人数为一半+1 而peer为不包含当前节点的集合 所以peer.size + 1为集群总数
        val quorum = (peers.size + 1) / 2 + 1
        //-1因为自己给自己投了一票
        val count = AtomicInteger(quorum - 1)
        val allowNextPromise = Promise.promise<Unit>()
        if (count.get() == 0) {
            allowNextPromise.complete()
        }
        val allowNext = AtomicBoolean(false)
        val allFutures = mutableListOf<Future<RequestVoteReply>>()
        for (address in peers) {
            val requestVoteReplyFuture = rpc.requestVote(address.value.SocketAddress(), buffer)
                .onSuccess {
                    val raft = this
                    if (it.isVoteGranted) {
                        //如果节点投了赞成票 isVoteGranted为true
                        //显示已经得到的赞成票数量和是否允许进入下一轮
                        raftLog("get vote from ${address.key}, count: ${count.get()} allowNex: ${allowNext}}")
                        //如果赞成票数量降到0，并且还没有进入下一轮，那么就会解锁allowNextPromise，允许进入下一轮。
                        if (count.decrementAndGet() == 0 && allowNext.compareAndSet(false, true)) {
                            allowNextPromise.complete()
                        }
                        return@onSuccess
                    }
                    if (it.term > currentTerm) {
                        becomeFollower(it.term)
                    }
                }

            allFutures += requestVoteReplyFuture
        }
        allowNextPromise.future()
            .onComplete {
                raftLog("allow to next, start checking status")
                val raft = this
                if (status == RaftStatus.candidate) {
                    raftLog("${me} become leader")
                    becomeLead()
                }
            }
    }


    internal fun becomeFollower(term: Int) {
        status = RaftStatus.follower
        currentTerm = term
        votedFor = null
        lastHearBeat = System.currentTimeMillis()
    }

    fun becomeCandidate() {
        currentTerm++
        votedFor = me
        lastHearBeat = System.currentTimeMillis()
        status = RaftStatus.candidate
    }

    fun becomeLead() {
        status = RaftStatus.leader
        lastHearBeat = System.currentTimeMillis()
        nextIndexes = mutableMapOf()
        matchIndexes = mutableMapOf()
        peers.forEach {
            nextIndexes[it.key] = IntAdder(stateMachine.getNowLogIndex() + 1)
            matchIndexes[it.key] = IntAdder(0)
        }
        leadId = me;

        //添加一个空日志 论文要求的
        addLog(NoopCommand())
        //不断心跳
        CoroutineScope(context.dispatcher() as CoroutineContext).launch {
            while (status == RaftStatus.leader) {
                broadcastLog()
                delay(heartBeat)
            }
        }

    }


    private fun broadcastLog(): MutableList<Future<AppendReply>> {
        val list = mutableListOf<Future<AppendReply>>()
        if (peers.isEmpty()) {
            val oldCommitindex = commitIndex
            calCommitIndex()
            if (oldCommitindex != commitIndex) {
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
        peerServerId: ServerId
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
            // next rf0->1
            //从哪开始即左边界（包含）
            val slice = stateMachine.sliceLogs(nextIndexValue)
            val term = getTermByIndex(nextIndexValue - 1)
            AppendRequest(currentTerm, nextIndexValue - 1, term, commitIndex, slice)
        }

        return rpc.appendRequest(peer.value.SocketAddress(), ar).onSuccess {
            if (it.isSuccess) {
                matchIndexes[peerServerId]?.value = logIndexSnap
                nextIndexes[peerServerId]?.value = logIndexSnap + 1
                val oldCommitindex = commitIndex
                calCommitIndex()
                if (oldCommitindex != commitIndex) {
                    stateMachine.applyLog(commitIndex)
                }
            } else {
                if (it.term > currentTerm) {
                    becomeFollower(it.term)
                } else {
                    adjustNextIndex(peerServerId)
                }
            }
        }
    }

    //1 2 3  -> 2
    //1 2 -> 1
    private fun calCommitIndex() {
        val values = matchIndexes.values.toMutableList()
        values.add(IntAdder(stateMachine.getNowLogIndex()))
        val list = values.sortedBy(IntAdder::value)
        val index = if (list.size % 2 == 0) {
            list.size / 2 - 1
        } else {
            list.size / 2
        }
        commitIndex = list[index].value
    }


    private fun adjustNextIndex(serverId: ServerId) {
        //todo 完善一下
        nextIndexes[serverId]?.add(-1)
    }

    fun getTermByIndex(index: Int) = stateMachine.getTermByIndex(index)

    fun insertLogs(prevIndex: Int, logs: List<Log>) = stateMachine.insertLogs(prevIndex, logs)

    fun getNowLogIndex(): Int {
        return stateMachine.getNowLogIndex()
    }

    fun getLastLogTerm(): Int {
        return stateMachine.getLastLogTerm()
    }

    fun close() {
        singleThreadVertx.close()
    }

    fun raftLog(msg: String) {
        if (!enablePrintInfo) {
            return
        }
        println("[${LocalDateTime.now()} serverId:$me term:$currentTerm index = ${stateMachine.getNowLogIndex()} status:${status} voteFor: ${votedFor}]: message:$msg")
    }

    /**
     * 外部调用的一个接口所以要确保线程安全
     */
    @NonBlocking
    @SwitchThread(Raft::class)
    fun addLog(command: Command, promise: Promise<Unit>) {
        if (leadId != me) {
            promise.fail(
                NotLeaderException(
                    "not leader!",
                    peers[leadId]
                )
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

    fun addServer(request: AddServerRequest, promise: Promise<Map<ServerId, RaftAddress>>) {
        if (leadId != me) {
            promise.fail(
                NotLeaderException(
                    "not leader!",
                    peers[leadId]
                )
            )
            return
        }
        val applyConfigPromise = Promise.promise<Unit>()
        applyConfigPromise.future()
            .onComplete {
                val res = peers.toMutableMap()
                res.remove(request.serverId)
                promise.complete(res)
            }
        val serverConfigChangeCommand = ServerConfigChangeCommand.create(request)
        context.runOnContext {
            addLog(serverConfigChangeCommand, applyConfigPromise)
        }
    }


    @NonBlocking
    @SwitchThread(Raft::class)
    fun lineRead(key: ByteArray, promise: Promise<Buffer>) {
        if (leadId != me) {
            promise.fail(
                NotLeaderException(
                    "not leader!",
                    peers[leadId]
                )
            )
            return
        }
        context.runOnContext {
            val readIndex = commitIndex
            val waiters = broadcastLog()
            val downLatch = CountDownLatch(waiters.size / 2)
            waiters.forEach { f ->
                f.onComplete {
                    downLatch.countDown()
                }
            }
            CoroutineScope(context.dispatcher() as CoroutineContext).launch {
                downLatch.wait()
                if (status != RaftStatus.leader) {
                    promise.fail(
                        NotLeaderException(
                            "not leader!",
                            peers[leadId]
                        )
                    )
                    return@launch
                }
                val readAction = if (key.isEmpty()) {
                    { promise.complete(SimpleKVStateMachineCodec.encode(stateMachine.db)) }
                } else {
                    { promise.complete(wrap(stateMachine.getDirect(key))) }
                }
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