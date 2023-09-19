import com.yxyl.raft.base.raft.RaftAddress
import com.yxyl.raft.base.utils.initJacksonMapper
import com.yxyl.configuration.Configuration
import com.yxyl.raft.base.kv.ByteArrayKey
import com.yxyl.raft.base.raft.RaftStatus
import com.yxyl.raft.base.utils.block
import com.yxyl.raft.client.RaftClient
import com.yxyl.server.RaftServer
import io.vertx.core.CompositeFuture
import io.vertx.core.Vertx
import io.vertx.core.net.SocketAddress
import org.junit.Assert
import org.junit.Test

class RaftServerTest {

    @Test
    fun testServer() {
        initJacksonMapper()
        val httpPort = 8080
        val configurations = mutableListOf<Configuration>()
        val vertx = Vertx.vertx()
        val nodes = mapOf(
            "raft-0" to RaftAddress(18081, "localhost"),
            "raft-1" to RaftAddress(18082, "localhost"),
            "raft-2" to RaftAddress(18083, "localhost")
        )

        //初始化 configurations
        for (i in (0 .. 2)) {
            val nodeId = "raft-$i"
            configurations.add(
                Configuration(
                    nodeId,
                    nodes[nodeId]!!.port,
                    httpPort + i,
                    mapOf(),
                    HashMap(nodes).apply { remove(nodeId) })
            )
        }

        val servers = configurations.map { RaftServer(it) }

        CompositeFuture.all(servers.map(RaftServer::start)).block()
        Thread.sleep(2000)
        val leaders = servers.filter { it.raft.status == RaftStatus.leader }

        Assert.assertEquals(1, leaders.size)
        val leaderServer = leaders[0]
        println("leader is ${leaderServer.raft.me}")

        //创建客户端模拟发送请求
        val client = RaftClient(
            vertx,
            SocketAddress.inetSocketAddress(leaderServer.configuration.httpPort, "localhost")
        )

        val snap = client.peekRaft().block()
        val raft = leaderServer.raft
        Assert.assertEquals(raft.commitIndex, snap.raftState.commitIndex)

        // get测试
        val key = "key1".toByteArray()
        val value = "value1".toByteArray()
        val buffer = client.set(key, value).block()

        val db = raft.stateMachine.db
        Assert.assertArrayEquals(value, db[ByteArrayKey(key)])
        var getRes = client.get(key).block()

        Assert.assertFalse(getRes.hasError)
        Assert.assertArrayEquals(value, getRes.value.bytes)
        println("set command success, the value that client gotten is ${String(getRes.value.bytes)}")

        // del测试
        val delRes = client.del(key).block()
        Assert.assertNull(db[ByteArrayKey(key)])
        getRes = client.get(key).block()
        Assert.assertEquals(0, getRes.value.length())
        println("after del, the value that client gotten is ${String(getRes.value.bytes)}")

    }

}