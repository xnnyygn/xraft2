package `in`.xnnyygn.xraft2.election

import `in`.xnnyygn.xraft2.NodeState
import `in`.xnnyygn.xraft2.NodeStateFileCell
import `in`.xnnyygn.xraft2.NodeStateLoadedEvent
import `in`.xnnyygn.xraft2.NodeStateUpdatedEvent
import `in`.xnnyygn.xraft2.cell.*
import `in`.xnnyygn.xraft2.net.*
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit

class ElectionCell(
    private val nodeName: String,
    private val addresses: MutableList<NodeAddress>,
    private val raftLog: CellRef,
    private val connectionSet: CellRef
) : Cell() {
    private var _nodeStateFile: CellRef? = null
    private var electionState: ElectionState = FollowerState(1, null)
    private var electionTimeout: ScheduledFuture<*>? = null

    private val nodeStateFile: CellRef
        get() = _nodeStateFile!!

    override fun start(context: CellContext) {
        _nodeStateFile = context.startChild(NodeStateFileCell())
    }

    override fun receive(context: CellContext, event: Event) {
        when (event) {
            is NodeStateLoadedEvent -> nodeStateLoaded(context, event.state)
            EnableElectionEvent -> scheduleTimeout(context)
            ElectionTimeoutEvent -> electionTimeout(context)
            is ConnectionMessageEvent -> connectionMessage(context, event)
            is LastLogMetaDataEvent -> lastLogMetaData(context, event)
            is CompareLastLogResultEvent -> compareLastLogResult(context, event)
            is HigherTermEvent -> higherTerm(context, event.term)
        }
    }

    private fun connectionMessage(context: CellContext, event: ConnectionMessageEvent) {
        when (val message = event.message) {
            is RequestVoteRpc -> requestVoteRpc(context, event, message)
            is RequestVoteReply -> requestVoteReply(context, message)
        }
    }

    private fun electionTimeout(context: CellContext) {
        electionTimeout = null
        val newTerm = electionState.term + 1
        electionState = CandidateState(newTerm)
        updateNodeState(newTerm, null)
        raftLog.tell(GetLastLogMetaDataCellEvent(context.self))
        // continue in lastLogData
    }

    private fun lastLogMetaData(context: CellContext, event: LastLogMetaDataEvent) {
        val message = RequestVoteRpc(electionState.term, nodeName, event.lastLogIndex, event.lastLogTerm)
        // broadcast RequestVoteRpc
        connectionSet.tell(PeerMessageEvent(message))
        scheduleTimeout(context)
    }

    private fun requestVoteReply(context: CellContext, reply: RequestVoteReply) {
        val state = this.electionState
        if (reply.term < state.term) {
            return
        }
        if (reply.term > state.term) {
            becomeFollower(context, reply.term, null)
            return
        }
        if (state !is CandidateState) {
            // a node might become leader before receiving replies from all nodes
            return
        }
        clearTimeout()
        if (reply.voteGranted && state.increaseVoteCount() > (addresses.size / 2)) {
            this.electionState = LeaderState(state.term) // candidate -> leader
            // TODO add raft log
            connectionSet.tell(EnableLogReplicationEvent(state.term))
        } else {
            scheduleTimeout(context)
        }
    }

    private fun becomeFollower(context: CellContext, term: Int, votedFor: String?) {
        if (electionState.role == Role.LEADER) {
            connectionSet.tell(DisableLogReplicationEvent)
        }
        clearTimeout()
        electionState = FollowerState(term, votedFor)
        updateNodeState(term, votedFor)
        scheduleTimeout(context)
    }

    private fun requestVoteRpc(context: CellContext, event: ConnectionMessageEvent, rpc: RequestVoteRpc) {
        val state = electionState
        if (rpc.term < state.term) {
            event.reply(RequestVoteReply(state.term, false))
            return
        }
        if (rpc.term == state.term) {
            if (state !is FollowerState) {
                // in split-vote, a candidate might send rpc to another candidate
                // also in split-vote, a candidate might become leader before receiving rpc from another candidate
                event.reply(RequestVoteReply(state.term, false))
            } else if (state.votedFor != null) {
                event.reply(RequestVoteReply(state.term, (state.votedFor == rpc.candidateId)))
            }
            return
        }
        raftLog.tell(CompareLastLogCellEvent(event.connection, rpc, context.self))
        // continue in compareLastLogResult
    }

    private fun compareLastLogResult(context: CellContext, event: CompareLastLogResultEvent) {
        val state = electionState
        val rpc = event.rpc
        if (rpc.term < state.term) {
            event.reply(RequestVoteReply(state.term, false))
            return
        }
        if (rpc.term > state.term) {
            if (event.result) {
                becomeFollower(context, rpc.term, rpc.candidateId)
                event.reply(RequestVoteReply(rpc.term, true))
            } else {
                becomeFollower(context, rpc.term, null)
                event.reply(RequestVoteReply(rpc.term, false))
            }
            return
        }
        // rpc.term == state.term
        clearTimeout()
        if (!event.result || (state !is FollowerState)) {
            event.reply(RequestVoteReply(state.term, false))
        } else if (state.votedFor != null) {
            event.reply(RequestVoteReply(state.term, (state.votedFor == rpc.candidateId)))
        } else { // vote == true && state.votedFor == null
            state.votedFor = rpc.candidateId
            event.reply(RequestVoteReply(state.term, true))
            updateNodeState(state.term, rpc.candidateId)
        }
        scheduleTimeout(context)
    }

    private fun higherTerm(context: CellContext, term: Int) {
        if (term > electionState.term) {
            becomeFollower(context, term, null)
        }
    }

    private fun nodeStateLoaded(context: CellContext, state: NodeState) {
        this.electionState = FollowerState(state)
        context.parent.tell(ElectionInitializedEvent)
    }

    private fun updateNodeState(term: Int, votedFor: String?) {
        nodeStateFile.tell(NodeStateUpdatedEvent(NodeState(term, votedFor)))
    }

    private fun scheduleTimeout(context: CellContext) {
        electionTimeout = context.schedule(1, TimeUnit.SECONDS, ElectionTimeoutEvent)
    }

    private fun clearTimeout() {
        val timeout = electionTimeout
        if (timeout != null) {
            timeout.cancel(true)
            this.electionTimeout = null
        }
    }

    override fun stop(context: CellContext) {
        clearTimeout()
    }
}


object ElectionInitializedEvent : Event
object EnableElectionEvent : Event
internal object ElectionTimeoutEvent : Event

class GetLastLogMetaDataCellEvent(sender: CellRef) : CellEvent(sender)
class LastLogMetaDataEvent(val lastLogIndex: Int, val lastLogTerm: Int) : Event
class CompareLastLogCellEvent(private val connection: CellRef, val rpc: RequestVoteRpc, sender: CellRef) :
    CellEvent(sender) {
    fun reply(result: Boolean) {
        reply(CompareLastLogResultEvent(connection, rpc, result))
    }
}

class CompareLastLogResultEvent(private val connection: CellRef, val rpc: RequestVoteRpc, val result: Boolean) : Event {
    fun reply(message: RequestVoteReply) {
        connection.tell(PeerMessageEvent(message))
    }
}

class HigherTermEvent(val term: Int) : Event