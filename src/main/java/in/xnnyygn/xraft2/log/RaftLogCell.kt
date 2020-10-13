package `in`.xnnyygn.xraft2.log

import `in`.xnnyygn.xraft2.cell.*
import `in`.xnnyygn.xraft2.election.LogSyncAppendEntriesCellEvent
import `in`.xnnyygn.xraft2.net.DisablePeerLogReplicatorCellEvent
import `in`.xnnyygn.xraft2.net.NodeAddress

class RaftLogCell : Cell() {
    private var replicator: CellRef? = null
    private var _raftLog: RaftLog? = null

    private val raftLog: RaftLog
        get() = _raftLog!!

    override fun start(context: CellContext) {
        // sync
        _raftLog = RaftLog(EmptyLogSequence, EmptySnapshot)
        context.parent.tell(LogInitializedEvent)
    }

    override fun receive(context: CellContext, event: Event) {
        when (event) {
            is EnableLogReplicatorCellEvent -> enableReplicator(context, event)
            is DisablePeerLogReplicatorCellEvent -> disableReplicator(context)
            is LogSyncAppendEntriesCellEvent -> {
            }
            // TODO register commit index listener
        }
    }

    private fun enableReplicator(context: CellContext, event: EnableLogReplicatorCellEvent) {
        if (replicator == null) {
            // TODO node list
            val replicator = context.startChild(LogReplicatorCell(event.term))
            this.replicator = replicator
            event.reply(emptyList(), replicator)
        } else {
            context.logger.warn("replicator has been enabled")
        }
    }

    private fun disableReplicator(context: CellContext) {
        val replicator = this.replicator
        if (replicator == null) {
            context.logger.warn("replicator isn't enabled")
        } else {
            replicator.tell(PoisonPill)
            this.replicator = null
        }
    }
}

object LogInitializedEvent : Event

class EnableLogReplicatorCellEvent(val nodeName: String, val term: Int, sender: CellRef) : CellEvent(sender) {
    fun reply(addresses: List<NodeAddress>, replicator: CellRef) {
        reply(EnableLogReplicatorReplyEvent(addresses, replicator))
    }
}

class EnableLogReplicatorReplyEvent(val addresses: List<NodeAddress>, val replicator: CellRef) : Event

class DisableLogReplicatorCellEvent(sender: CellRef): CellEvent(sender) {
    fun reply() {
        reply(LogReplicatorDisabledEvent)
    }
}

object LogReplicatorDisabledEvent: Event