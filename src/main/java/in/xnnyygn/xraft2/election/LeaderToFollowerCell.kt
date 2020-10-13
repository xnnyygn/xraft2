package `in`.xnnyygn.xraft2.election

import `in`.xnnyygn.xraft2.DisableNodeListUpdateCellEvent
import `in`.xnnyygn.xraft2.NodeListUpdateDisabledEvent
import `in`.xnnyygn.xraft2.cell.Cell
import `in`.xnnyygn.xraft2.cell.CellContext
import `in`.xnnyygn.xraft2.cell.CellRef
import `in`.xnnyygn.xraft2.cell.Event
import `in`.xnnyygn.xraft2.log.DisableLogReplicatorCellEvent
import `in`.xnnyygn.xraft2.log.LogReplicatorDisabledEvent
import `in`.xnnyygn.xraft2.net.DisablePeerLogReplicatorCellEvent
import `in`.xnnyygn.xraft2.net.DisablePeerLogReplicatorReplyEvent
import `in`.xnnyygn.xraft2.net.NodeAddress
import `in`.xnnyygn.xraft2.net.PeerLogReplicatorDisabledEvent

class LeaderToFollowerCell(
    private val raftLog: CellRef,
    private val connectionSet: CellRef,
    private val nodeList: CellRef,
    private val action: SendEventAction? = null
) : Cell() {
    private var remainingPeerSet: MutableSet<NodeAddress> = mutableSetOf()

    override fun start(context: CellContext) {
        nodeList.tell(DisableNodeListUpdateCellEvent(context.self))
    }

    override fun receive(context: CellContext, event: Event) {
        when (event) {
            is NodeListUpdateDisabledEvent -> nodeListUpdateDisabled(context)
            is DisablePeerLogReplicatorReplyEvent -> disablePeerLogReplicatorReply(context, event)
            is PeerLogReplicatorDisabledEvent -> peerLogReplicatorDisabled(context, event)
            is LogReplicatorDisabledEvent -> logReplicatorDisabled(context)
        }
    }

    private fun nodeListUpdateDisabled(context: CellContext) {
        connectionSet.tell(DisablePeerLogReplicatorCellEvent(context.self))
    }

    private fun disablePeerLogReplicatorReply(context: CellContext, event: DisablePeerLogReplicatorReplyEvent) {
        val peerSet = event.addressSet.toMutableSet()
        for (peer in event.unavailable) {
            peerSet.remove(peer)
        }
        this.remainingPeerSet = peerSet
        checkIfAllPeerDisabled(context)
    }

    private fun checkIfAllPeerDisabled(context: CellContext) {
        if (this.remainingPeerSet.isEmpty()) {
            raftLog.tell(DisableLogReplicatorCellEvent(context.self))
        }
    }

    private fun peerLogReplicatorDisabled(context: CellContext, event: PeerLogReplicatorDisabledEvent) {
        remainingPeerSet.remove(event.address)
        checkIfAllPeerDisabled(context)
    }

    private fun logReplicatorDisabled(context: CellContext) {
        action?.run()
        context.resumeParent()
        context.stopSelf()
    }
}

class SendEventAction(val receiver: CellRef, val event: Event) {
    fun run() {
        receiver.tell(event)
    }
}