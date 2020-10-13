package `in`.xnnyygn.xraft2.election

import `in`.xnnyygn.xraft2.*
import `in`.xnnyygn.xraft2.cell.*
import `in`.xnnyygn.xraft2.net.*

class ElectionCell(
    private val nodeName: String,
    private val raftLog: CellRef,
    private val connectionSet: CellRef
) : Cell() {
    private var _nodeStateFile: CellRef? = null
    private var _nodeList: CellRef? = null
    private var nodeNameSet: Set<String> = emptySet()
    private var state: ElectionState = FollowerState(1, null)
    private val timeout = ElectionTimeout(1000)

    /**
     * update addresses
     * if nodeName not in addresses, stop self
     */

    private val nodeStateFile: CellRef
        get() = _nodeStateFile!!

    private val nodeList: CellRef
        get() = _nodeList ?: EmptyCellRef

    override fun start(context: CellContext) {
        _nodeStateFile = context.startChild(NodeStateFileCell())
    }

    override fun receive(context: CellContext, event: Event) {
        when (event) {
            // from NodeStateFileCell
            is NodeStateLoadedEvent -> nodeStateLoaded(context, event.state)
            // from NodeListCell
            is RegisterNodeListEvent -> _nodeList = event.nodeList
            // from InitializerCell
            EnableElectionEvent -> timeout.schedule(context)
            ElectionTimeoutEvent -> electionTimeout(context)
            is ConnectionMessageEvent -> connectionMessage(context, event)
            is HigherTermEvent -> higherTerm(context, event.term)
            // continue
            // from RaftLogCell
            is DataForCandidateEvent -> dataForCandidate(context, event)
            is CompareLastLogResultEvent -> compareLastLogResult(context, event)
        }
    }

    private fun connectionMessage(context: CellContext, event: ConnectionMessageEvent) {
        when (val message = event.message) {
            is RequestVoteRpc -> requestVoteRpc(context, event, message)
            is RequestVoteReply -> requestVoteReply(context, message)
            is AppendEntriesRpc -> appendEntriesRpc(context, event, message)
        }
    }

    private fun electionTimeout(context: CellContext) {
        if (!timeout.isValid) {
            /**
             * To checking validity is necessary here.
             * Given a slow node, it replies at the same time of election timeout.
             * If reply from the slow node is processed before election timeout, it might be a problem.
             * Previous election timeout might enqueue the ElectionTimeoutEvent while system is processing.
             * Cancelling election timeout at first won't help since enqueuing the event is in the scheduler thread.
             * Election timeout will still be triggered but we can check the intended time we scheduled.
             *
             *   currentTime - intendedScheduledAt > delay
             *
             * If it's too short, we know this election timeout isn't an expected one,
             * it's a previous election timeout we cannot cancel.
             *
             * Moreover, we cannot set the underlying ScheduledFuture to null even though it seems we can do that.
             * In the case of the slow node, the underlying ScheduledFuture is not the one triggering election timeout,
             * so we cannot set it to null.
             */
            context.logger.warn("outdated election timeout, skip")
            return
        }
        timeout.clear()
        val newTerm = state.term + 1
        state = CandidateState(newTerm)
        updateNodeState(newTerm, null)
        /**
         * Since processing of AppendEntriesRpc in raft log is synchronized, the operation to get last log will
         * retrieve the latest value.
         * After node becomes a candidate, any AppendEntriesRpc will turn node to be a follower and
         * any AddPeerTask or RemovePeerTask will fail when node is a candidate, so the node addresses here won't
         * be changed during the period of being a candidate
         */
        raftLog.tell(GetDataForCandidateEvent(context))
        context.suspendBy(raftLog)
        // continue in dataForCandidate
    }

    private fun dataForCandidate(context: CellContext, event: DataForCandidateEvent) {
        nodeNameSet = event.addresses.map { it.name }.toSet()
        val message = RequestVoteRpc(
            state.term,
            nodeName,
            event.lastLogIndex,
            event.lastLogTerm
        )
        connectionSet.tell(PeerMessageEvent(message)) // broadcast RequestVoteRpc
        timeout.schedule(context)
    }

    private fun requestVoteReply(context: CellContext, reply: RequestVoteReply) {
        // TODO node not in nodeNameSet?
        val state = this.state
        if (reply.term < state.term) {
            return
        }
        if (reply.term > state.term) {
            becomeFollower(context, FollowerState(reply.term, null))
            return
        }
        if (state !is CandidateState) {
            // a node might become leader before receiving replies from all nodes
            return
        }
        timeout.cancel(context)
        if (reply.voteGranted && state.increaseVoteCount() > (nodeNameSet.size / 2)) {
            this.state = LeaderState(state.term) // candidate -> leader
            context.suspendBy(CandidateToLeaderCell(nodeName, state.term, raftLog, connectionSet))
        } else {
            timeout.schedule(context)
        }
    }

    private fun becomeFollowerAndSendEvent(
        context: CellContext,
        follower: FollowerState,
        receiver: CellRef,
        event: Event
    ) {
        timeout.cancel(context)
        if (state.role == Role.LEADER) {
            val action = SendEventAction(receiver, event)
            context.suspendBy(LeaderToFollowerCell(raftLog, connectionSet, nodeList, action))
        }
        initializeFollower(context, follower)
    }

    private fun becomeFollower(context: CellContext, follower: FollowerState) {
        timeout.cancel(context)
        if (state.role == Role.LEADER) {
            context.suspendBy(LeaderToFollowerCell(raftLog, connectionSet, nodeList))
        }
        /**
         * It's OK to initialize follower here while LeaderToFollowerCell is running.
         * Election is suspended and its status won't be changed.
         * The result of running LeaderToFollowerCell first, then initializing is
         * the same as running both at the same time.
         */
        initializeFollower(context, follower)
    }

    private fun initializeFollower(context: CellContext, follower: FollowerState) {
        state = follower
        updateNodeState(follower.term, follower.votedFor)
        timeout.schedule(context)
    }

    private fun requestVoteRpc(context: CellContext, event: ConnectionMessageEvent, rpc: RequestVoteRpc) {
        val state = state
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
        raftLog.tell(CompareLastLogEvent(event.connection, rpc, context))
        context.suspendBy(raftLog)
        // continue in compareLastLogResult
    }

    private fun compareLastLogResult(context: CellContext, event: CompareLastLogResultEvent) {
        val state = state
        val rpc = event.rpc
        if (rpc.term < state.term) {
            event.reply(RequestVoteReply(state.term, false))
            return
        }
        if (rpc.term > state.term) {
            if (event.result) {
                becomeFollowerAndSendEvent(
                    context,
                    FollowerState(rpc.term, rpc.candidateId),
                    event.connection,
                    PeerMessageEvent(RequestVoteReply(rpc.term, true))
                )
            } else {
                becomeFollowerAndSendEvent(
                    context,
                    FollowerState(rpc.term, null),
                    event.connection,
                    PeerMessageEvent(RequestVoteReply(rpc.term, false))
                )
            }
            return
        }
        // rpc.term == state.term
        timeout.cancel(context)
        if (!event.result || (state !is FollowerState)) {
            event.reply(RequestVoteReply(state.term, false))
        } else if (state.votedFor != null) {
            event.reply(RequestVoteReply(state.term, (state.votedFor == rpc.candidateId)))
        } else { // vote == true && state.votedFor == null
            state.votedFor = rpc.candidateId
            event.reply(RequestVoteReply(state.term, true))
            updateNodeState(state.term, rpc.candidateId)
        }
        timeout.schedule(context)
    }

    private fun higherTerm(context: CellContext, term: Int) {
        if (term > state.term) {
            becomeFollower(context, FollowerState(term, null))
        }
    }

    private fun appendEntriesRpc(context: CellContext, event: ConnectionMessageEvent, rpc: AppendEntriesRpc) {
        val term = state.term
        if (rpc.term < term) {
            event.reply(AppendEntriesReply(term, false))
            return
        }
        if (rpc.term > term) {
            becomeFollowerAndSendEvent(
                context,
                FollowerState(rpc.term, null),
                raftLog,
                LogSyncAppendEntriesCellEvent(event.connection, rpc, rpc.term)
            )
            return
        }
        if (state is FollowerState) {
            // TODO set leader id
            // forward
            raftLog.tell(LogSyncAppendEntriesCellEvent(event.connection, rpc, term))
        } else if (state.role == Role.CANDIDATE) {
            becomeFollowerAndSendEvent(
                context,
                FollowerState(term, null),
                raftLog,
                LogSyncAppendEntriesCellEvent(event.connection, rpc, term)
            )
        } else { // leader
            event.reply(AppendEntriesReply(term, false))
        }
    }

    private fun nodeStateLoaded(context: CellContext, state: NodeState) {
        this.state = FollowerState(state)
        context.parent.tell(ElectionInitializedEvent)
    }

    private fun updateNodeState(term: Int, votedFor: String?) {
        nodeStateFile.tell(NodeStateUpdatedEvent(NodeState(term, votedFor)))
    }

    override fun stop(context: CellContext) {
        timeout.cancel(context)
    }
}

object ElectionInitializedEvent : Event
object EnableElectionEvent : Event
internal object ElectionTimeoutEvent : Event

class GetDataForCandidateEvent(private val context: CellContext) : Event {
    fun reply(event: DataForCandidateEvent, sender: CellRef) {
        context.resume(sender, event)
    }
}

class DataForCandidateEvent(val addresses: List<NodeAddress>, val lastLogIndex: Int, val lastLogTerm: Int) : Event

class CompareLastLogEvent(
    private val connection: CellRef,
    val rpc: RequestVoteRpc,
    private val context: CellContext
) : Event {
    fun reply(result: Boolean, sender: CellRef) {
        context.resume(sender, CompareLastLogResultEvent(connection, rpc, result))
    }
}

class CompareLastLogResultEvent(val connection: CellRef, val rpc: RequestVoteRpc, val result: Boolean) : Event {
    fun reply(message: RequestVoteReply) {
        connection.tell(PeerMessageEvent(message))
    }
}

class HigherTermEvent(val term: Int) : Event

class LogSyncAppendEntriesCellEvent(
    private val connection: CellRef,
    val rpc: AppendEntriesRpc,
    val term: Int
) : CellEvent(connection) {
    fun reply(success: Boolean) {
        reply(PeerMessageEvent(AppendEntriesReply(term, success)))
    }
}