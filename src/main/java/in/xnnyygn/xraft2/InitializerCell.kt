package `in`.xnnyygn.xraft2

import `in`.xnnyygn.xraft2.cell.Cell
import `in`.xnnyygn.xraft2.cell.CellContext
import `in`.xnnyygn.xraft2.cell.CellRef
import `in`.xnnyygn.xraft2.cell.Event
import `in`.xnnyygn.xraft2.election.ElectionCell
import `in`.xnnyygn.xraft2.election.ElectionInitializedEvent
import `in`.xnnyygn.xraft2.election.EnableElectionEvent
import `in`.xnnyygn.xraft2.log.LogInitializedEvent
import `in`.xnnyygn.xraft2.log.RaftLogCell
import `in`.xnnyygn.xraft2.net.*
import io.netty.channel.nio.NioEventLoopGroup

class InitializerCell(
    private val workerGroup: NioEventLoopGroup
) : Cell() {
    private var connections: CellRef? = null
    private var election: CellRef? = null
    private var electionInitialized = false
    private var logInitialized = false

    /**
     * Initializing flow:
     * -> (election, log)
     * -> (acceptor)
     * -> enable election
     *
     * @see receive
     * @see electionOrLogInitialized
     */
    override fun start(context: CellContext) {
        val connectionSet = context.startChild(ConnectionSetCell("A", workerGroup))
        val raftLog = context.startChild(RaftLogCell())
        val election = context.startChild(ElectionCell("A", raftLog, connectionSet))
        /**
         * load group configs from log
         * notify node list, node list update connection set
         */
        // for new peer
        // leader -> AddNewPeer, group config committed
        // follower -> AppendEntriesRpc, group config added -> group config committed
        context.startChild(NodeListCell(raftLog, connectionSet, election))

        this.connections = connectionSet
        this.election = election
    }

    override fun receive(context: CellContext, event: Event) {
        if (event == ElectionInitializedEvent) {
            electionInitialized = true
            electionOrLogInitialized(context)
        } else if (event == LogInitializedEvent) {
            logInitialized = true
            electionOrLogInitialized(context)
        } else if (event == ServerInitializedEvent) {
            context.logger.info("enable election")
            election!!.tell(EnableElectionEvent)
        } else if (event == ServerInitializationFailedEvent) {
            context.stopSelf()
        }
    }

    private fun electionOrLogInitialized(context: CellContext) {
        if (electionInitialized && logInitialized) {
            context.logger.debug("election and log initialized")
            context.startChild(ServerCell(NodeAddress("localhost", 2301), workerGroup, connections!!))
        }
    }
}