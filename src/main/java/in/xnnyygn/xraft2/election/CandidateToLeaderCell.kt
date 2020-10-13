package `in`.xnnyygn.xraft2.election

import `in`.xnnyygn.xraft2.cell.Cell
import `in`.xnnyygn.xraft2.cell.CellContext
import `in`.xnnyygn.xraft2.cell.CellRef
import `in`.xnnyygn.xraft2.cell.Event
import `in`.xnnyygn.xraft2.log.EnableLogReplicatorCellEvent
import `in`.xnnyygn.xraft2.log.EnableLogReplicatorReplyEvent
import `in`.xnnyygn.xraft2.net.EnablePeerLogReplicatorCellEvent
import `in`.xnnyygn.xraft2.net.EnablePeerLogReplicatorReplyEvent
import `in`.xnnyygn.xraft2.net.NodeAddress
import `in`.xnnyygn.xraft2.net.UnavailablePeerListEvent

class CandidateToLeaderCell(
    private val nodeName: String,
    private val term: Int,
    private val raftLog: CellRef,
    private val connectionSet: CellRef
) : Cell() {
    private var enabledPeerSet: MutableSet<NodeAddress>? = null

    override fun start(context: CellContext) {
        raftLog.tell(EnableLogReplicatorCellEvent(nodeName, term, context.self))
        // TODO add raft log
    }

    override fun receive(context: CellContext, event: Event) {
        when (event) {
            is EnableLogReplicatorReplyEvent -> enablePeerLogReplicator(context, event)
            is UnavailablePeerListEvent -> unavailablePeerList(context, event)
            is EnablePeerLogReplicatorReplyEvent -> peerLogReplicatorEnabled(context, event)
        }
    }

    private fun enablePeerLogReplicator(context: CellContext, event: EnableLogReplicatorReplyEvent) {
        val peers = event.addresses.filter { it.name != nodeName }
        this.enabledPeerSet = peers.toMutableSet()
        connectionSet.tell(
            EnablePeerLogReplicatorCellEvent(
                peers,
                term,
                raftLog,
                event.replicator,
                context.parent, // election
                context.self
            )
        )
    }

    private fun peerLogReplicatorEnabled(context: CellContext, event: EnablePeerLogReplicatorReplyEvent) {
        val set = this.enabledPeerSet
        if (set != null) {
            set.remove(event.address)
            checkIfDone(context, set)
        } else {
            context.logger.warn("peer set not ready")
        }
    }

    private fun checkIfDone(context: CellContext, peerSet: Set<NodeAddress>) {
        if (peerSet.isEmpty()) {
            context.resumeParent()
            context.stopSelf()
        }
    }

    private fun unavailablePeerList(context: CellContext, event: UnavailablePeerListEvent) {
        val peerSet = this.enabledPeerSet
        if (peerSet != null) {
            for (address in event.peers) {
                peerSet.remove(address)
            }
            checkIfDone(context, peerSet)
        } else
            context.logger.warn("peer set not ready")
    }
}