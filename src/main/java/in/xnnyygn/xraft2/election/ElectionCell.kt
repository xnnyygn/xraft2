package `in`.xnnyygn.xraft2.election

import `in`.xnnyygn.xraft2.NodeState
import `in`.xnnyygn.xraft2.NodeStateFileCell
import `in`.xnnyygn.xraft2.NodeStateLoadedEvent
import `in`.xnnyygn.xraft2.cell.Cell
import `in`.xnnyygn.xraft2.cell.CellContext
import `in`.xnnyygn.xraft2.cell.CellRef
import `in`.xnnyygn.xraft2.cell.CellEvent
import `in`.xnnyygn.xraft2.net.PeerMessageEvent
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

    override fun receive(context: CellContext, event: CellEvent) {
        if (event is NodeStateLoadedEvent) {
            _election = Election(event.state, nodeStateFile!!, connections)
            context.parent.tell(ElectionInitializedEvent)
        } else if (event == EnableElectionEvent) {
            election.scheduleTimeout(context)
        } else if (event == ElectionTimeoutEvent) {
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
    private var electionTimeout: ScheduledFuture<*>? = null

    fun scheduleTimeout(context: CellContext) {
        electionTimeout = context.schedule(1, TimeUnit.SECONDS, ElectionTimeoutEvent)
    }

    fun electionTimeout() {
        // TODO become candidate
        electionState = CandidateState(electionState.term + 1)
        // TODO send request vote to all peers
        connections.tell(PeerMessageEvent(RequestVoteRpc()))
        // schedule election timeout
    }
}

/**
 * to [InitializerCell]
 */
object ElectionInitializedEvent : CellEvent

/**
 * from [InitializerCell]
 */
object EnableElectionEvent : CellEvent

object ElectionTimeoutEvent : CellEvent