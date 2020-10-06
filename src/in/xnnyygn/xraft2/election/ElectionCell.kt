package `in`.xnnyygn.xraft2.election

import `in`.xnnyygn.xraft2.NodeState
import `in`.xnnyygn.xraft2.NodeStateFileCell
import `in`.xnnyygn.xraft2.NodeStateLoadedMessage
import `in`.xnnyygn.xraft2.cell.Cell
import `in`.xnnyygn.xraft2.cell.CellContext
import `in`.xnnyygn.xraft2.cell.CellRef
import `in`.xnnyygn.xraft2.cell.Message
import `in`.xnnyygn.xraft2.net.PeerRpcMessage
import `in`.xnnyygn.xraft2.net.RequestVoteRpc
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit

class ElectionCell(private val connections: CellRef) : Cell() {
    private var nodeStateFile: CellRef? = null
    private var _election: Election? = null

    private val election: Election
        get() = _election!!

    override fun start(context: CellContext) {
        nodeStateFile = context.startChild(NodeStateFileCell())
    }

    override fun receive(context: CellContext, msg: Message) {
        if (msg is NodeStateLoadedMessage) {
            _election = Election(msg.state, nodeStateFile!!, connections)
            context.parent.send(ElectionInitializedMessage)
        } else if (msg == EnableElectionMessage) {
            election.scheduleTimeout(context)
        } else if (msg == ElectionTimeoutMessage) {
            election.electionTimeout()
        }
    }
}

class Election(
    nodeState: NodeState,
    private val nodeStateFile: CellRef,
    private val connections: CellRef
) {
    private var electionState: ElectionState = FollowerState(nodeState)
    private var electionTimeout: ScheduledFuture<Unit>? = null

    fun scheduleTimeout(context: CellContext) {
        electionTimeout = context.schedule(1, TimeUnit.SECONDS, ElectionTimeoutMessage)
    }

    fun electionTimeout() {
        // TODO become candidate
        electionState = CandidateState(electionState.term + 1)
        // TODO send request vote to all peers
        connections.send(PeerRpcMessage(RequestVoteRpc()))
        // schedule election timeout
    }
}

/**
 * to [InitializerCell]
 */
object ElectionInitializedMessage : Message

/**
 * from [InitializerCell]
 */
object EnableElectionMessage : Message

object ElectionTimeoutMessage : Message