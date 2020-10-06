package `in`.xnnyygn.xraft2

import `in`.xnnyygn.xraft2.cell.Cell
import `in`.xnnyygn.xraft2.cell.CellContext
import `in`.xnnyygn.xraft2.cell.CellRef
import `in`.xnnyygn.xraft2.cell.Message
import `in`.xnnyygn.xraft2.election.ElectionCell
import `in`.xnnyygn.xraft2.election.ElectionInitializedMessage
import `in`.xnnyygn.xraft2.election.EnableElectionMessage
import `in`.xnnyygn.xraft2.log.RaftLogCell
import `in`.xnnyygn.xraft2.log.LogInitializedMessage
import `in`.xnnyygn.xraft2.net.AcceptorCell
import `in`.xnnyygn.xraft2.net.AcceptorInitializedMessage
import `in`.xnnyygn.xraft2.net.ConnectionsCell

class InitializerCell : Cell() {
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
        val connections = context.startChild(ConnectionsCell())
        val election = context.startChild(ElectionCell(connections))
        val raftLog = context.startChild(RaftLogCell(connections))
        val serverList = context.startChild(ServerListCell())
        context.startChild(LogSynchronizerCell(election, raftLog, serverList))

        this.connections = connections
        this.election = election
    }

    override fun receive(context: CellContext, msg: Message) {
        if (msg == ElectionInitializedMessage) {
            electionInitialized = true
            electionOrLogInitialized(context)
        } else if (msg == LogInitializedMessage) {
            logInitialized = true
            electionOrLogInitialized(context)
        } else if (msg == AcceptorInitializedMessage) {
            election!!.send(EnableElectionMessage)
        }
    }

    private fun electionOrLogInitialized(context: CellContext) {
        if (electionInitialized && logInitialized) {
            context.startChild(AcceptorCell(connections!!))
        }
    }

    override fun stop(context: CellContext) {
        // TODO stop all children
    }
}