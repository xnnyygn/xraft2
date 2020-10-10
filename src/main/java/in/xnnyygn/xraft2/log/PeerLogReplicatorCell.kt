package `in`.xnnyygn.xraft2.log

import `in`.xnnyygn.xraft2.cell.*
import `in`.xnnyygn.xraft2.election.HigherTermEvent
import `in`.xnnyygn.xraft2.net.*
import io.netty.channel.Channel
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit

class PeerLogReplicatorCell(
    private val term: Int,
    private val address: NodeAddress,
    private val channel: Channel
) : Cell() {
    private var _raftLog: CellRef? = null
    private var _logReplicator: CellRef? = null
    private var _election: CellRef? = null
    private var matchIndex: Int = 0
    private var nextIndex: Int = -1
    private var lastIndex: Int = -1
    private var lastAppendEntriesRpc: AppendEntriesRpc? = null
    private var lastInstallSnapshotRpc: InstallSnapshotRpc? = null
    private var heartbeatTimeout: ScheduledFuture<*>? = null

    private val raftLog: CellRef
        get() = _raftLog!!

    private val logReplicator: CellRef
        get() = _logReplicator!!

    private val election: CellRef
        get() = _election!!

    override val name: String = "PeerLogReplicatorCell($address)"

    override fun start(context: CellContext) {
        val raftLog = context.findCell("/InitializerCell/RaftLogCell")
        raftLog.tell(GetLogDataForReplicationCellEvent(context.self))
        this._raftLog = raftLog
        _logReplicator = context.findCell(("/InitializerCell/RaftLogCell/LogReplicatorCell"))
        _election = context.findCell("/InitializerCell/ElectionCell")
    }

    override fun receive(context: CellContext, event: Event) {
        when (event) {
            is PeerMessageEvent -> handlePeerMessage(context, event.message)
            is LogDataForReplicationEvent -> sendPeerMessage(event)
            is Heartbeat -> heartbeat(context)
        }
    }

    private fun heartbeat(context: CellContext) {
        heartbeatTimeout = null
        raftLog.tell(GetLogDataForReplicationCellEvent(context.self, nextIndex))
    }

    private fun sendPeerMessage(event: LogDataForReplicationEvent) {
        val message = event.message
        if (message is AppendEntriesRpc) {
            if (nextIndex == -1) {
                nextIndex = message.prevLogIndex + 1
            }
            lastAppendEntriesRpc = message
        } else if (message is InstallSnapshotRpc) {
            if (nextIndex == -1) {
                nextIndex = message.lastIndex + 1
            }
            lastInstallSnapshotRpc = message
        }
        lastIndex = event.lastIndex
        channel.write(message)
    }

    private fun handlePeerMessage(context: CellContext, message: PeerMessage) {
        when (message) {
            is AppendEntriesReply -> appendEntriesReply(context, message)
            is InstallSnapshotReply -> installSnapshotReply(context, message)
        }
    }

    private fun installSnapshotReply(context: CellContext, message: InstallSnapshotReply) {
        if (message.term > term) {
            election.tell(HigherTermEvent(message.term))
            return
        }
        if (lastInstallSnapshotRpc == null) {
            context.logger.warn("unexpected install snapshot reply")
            return
        }
        // handle
    }

    private fun appendEntriesReply(context: CellContext, message: AppendEntriesReply) {
        if (message.term > term) {
            election.tell(HigherTermEvent(message.term))
            return
        }
        val lastRpc = lastAppendEntriesRpc
        if (lastRpc == null) {
            context.logger.warn("unexpected append entries reply")
            return
        }
        if (!message.success) {
            if (nextIndex > 1) {
                nextIndex--
                raftLog.tell(GetLogDataForReplicationCellEvent(context.self, nextIndex))
            } else {
                context.logger.warn("next index <= 1")
            }
            return
        }
        val oldMatchIndex = matchIndex
        matchIndex = lastRpc.prevLogIndex
        if (matchIndex != oldMatchIndex) {
            logReplicator.tell(MatchIndexUpdatedEvent(matchIndex, address))
        }
        if (matchIndex < lastIndex) {
            raftLog.tell(GetLogDataForReplicationCellEvent(context.self, nextIndex))
        } else {
            context.logger.debug("all logs match")
            heartbeatTimeout = context.schedule(1L, TimeUnit.MILLISECONDS, Heartbeat)
        }
    }

    override fun stop(context: CellContext) {
        heartbeatTimeout?.cancel(true)
    }
}

class GetLogDataForReplicationCellEvent(sender: CellRef, val nextIndex: Int = -1) : CellEvent(sender)
class LogDataForReplicationEvent(val lastIndex: Int, val message: PeerMessage) : Event

internal object Heartbeat : Event
class MatchIndexUpdatedEvent(val matchIndex: Int, val address: NodeAddress) : Event