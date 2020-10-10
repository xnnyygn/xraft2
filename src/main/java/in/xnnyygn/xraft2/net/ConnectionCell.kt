package `in`.xnnyygn.xraft2.net

import `in`.xnnyygn.xraft2.cell.*
import `in`.xnnyygn.xraft2.log.PeerLogReplicatorCell
import io.netty.channel.Channel

internal class ConnectionCell(
    private val address: NodeAddress,
    private val channel: Channel
) : Cell() {
    private var replicator: CellRef? = null

    override val name: String = "Connection<${address}>"

    override fun start(context: CellContext) {
        channel.pipeline().addLast(RaftHandler(address, context))
    }

    override fun receive(context: CellContext, event: Event) {
        when (event) {
            is PendingMessageEvent -> handlePendingMessage(context, event)
            is PeerMessageEvent -> channel.write(event.message)
            is PeerMessageForwardEvent -> forwardPeerMessage(context, event.message)
            is EnableLogReplicationEvent -> enableLogReplication(context)
            is DisableLogReplicationEvent -> disableLogReplication(context)
            is PoisonPill -> context.stopSelf()
        }
    }

    private fun enableLogReplication(context: CellContext) {
        if (replicator == null) {
            context.logger.info("enable log replication")
            replicator = context.startChild(PeerLogReplicatorCell(address, channel))
        }
    }

    private fun disableLogReplication(context: CellContext) {
        val replicator = this.replicator
        if (replicator != null) {
            context.logger.info("disable log replication")
            replicator.tell(PoisonPill)
            this.replicator = null
        }
    }

    private fun handlePendingMessage(context: CellContext, event: PendingMessageEvent) {
        if (event.queue != null) {
            channel.write(event.queue.lastMessage)
        }
        if (event.logReplicationEnabled) {
            enableLogReplication(context)
        } else {
            disableLogReplication(context)
        }
    }

    private fun forwardPeerMessage(context: CellContext, message: PeerMessage) {
        if (message !is AppendEntriesReply) {
            return
        }
        val replicator = this.replicator
        if (replicator == null) {
            context.logger.warn("log replication is disabled, skip message forwarding")
            return
        }
        replicator.tell(ConnectionMessageEvent(message, context.self))
    }


    override fun stop(context: CellContext) {
        if (channel.isOpen) {
            channel.close()
        }
    }
}

class PeerMessageEvent(val message: PeerMessage) : Event

class ConnectionMessageEvent(val message: PeerMessage, val connection: CellRef) : CellEvent(connection)

object EnableLogReplicationEvent : Event
object DisableLogReplicationEvent : Event

internal class PeerMessageForwardEvent(val message: PeerMessage) : Event
internal class ConnectionClosedEvent(val address: NodeAddress) : Event