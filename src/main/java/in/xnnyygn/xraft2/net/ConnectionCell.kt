package `in`.xnnyygn.xraft2.net

import `in`.xnnyygn.xraft2.cell.*
import `in`.xnnyygn.xraft2.getLogger
import `in`.xnnyygn.xraft2.log.PeerLogReplicatorCell
import io.netty.channel.Channel
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInboundHandlerAdapter

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
            is EnablePeerLogReplicatorCellEvent -> enableLogReplication(context, event.term)
            is DisablePeerLogReplicatorCellEvent -> disableLogReplication(context)
            // move PoisonPill to CellTask?
            is PoisonPill -> context.stopSelf()
        }
    }

    private fun enableLogReplication(context: CellContext, term: Int) {
        if (replicator == null) {
            context.logger.info("enable log replication")
            replicator = context.startChild(PeerLogReplicatorCell(term, address, channel))
        }
    }

    private fun disableLogReplication(context: CellContext) {
        val replicator = this.replicator
        if (replicator != null) {
            context.logger.info("disable log replication")
            // no need to suspend here
            replicator.tell(PoisonPill)
            this.replicator = null
        }
    }

    private fun handlePendingMessage(context: CellContext, event: PendingMessageEvent) {
        if (event.queue != null) {
            channel.write(event.queue.lastMessage)
        }
        when (val logReplicationEvent = event.logReplicationEvent) {
            is EnablePeerLogReplicatorCellEvent -> enableLogReplication(context, logReplicationEvent.term)
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
        replicator.tell(ConnectionMessageEvent(message, context.self, address.name))
    }


    override fun stop(context: CellContext) {
        if (channel.isOpen) {
            channel.close()
        }
    }
}

internal class RaftHandler(
    private val address: NodeAddress,
    context: CellContext // context of connection cell
) : ChannelInboundHandlerAdapter() {
    companion object {
        val logger = getLogger(RaftHandler::class.java)
    }

    private val election = context.findCell("/Election")
    private val connection = context.self

    override fun exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
        logger.warn("io error occurred", cause)
        ctx.close()
    }

    override fun channelRead(ctx: ChannelHandlerContext, msg: Any) {
        when (msg) {
            is RequestVoteRpc -> election.tell(ConnectionMessageEvent(msg, connection, address.name))
            is RequestVoteReply -> election.tell(ConnectionMessageEvent(msg, connection, address.name))
            is AppendEntriesRpc -> election.tell(ConnectionMessageEvent(msg, connection, address.name))
            is AppendEntriesReply -> connection.tell(PeerMessageForwardEvent(msg)) // TODO add address.name to Event
            else -> logger.warn { "unexpected message from $address: $msg" }
        }
    }
}

class PeerMessageEvent(val message: PeerMessage) : Event

class ConnectionMessageEvent(val message: PeerMessage, val connection: CellRef, val nodeName: String) :
    CellEvent(connection) {
    fun reply(message: PeerMessage) {
        reply(PeerMessageEvent(message))
    }
}

// TODO move to connection set
class EnablePeerLogReplicatorCellEvent(
    val peers: List<NodeAddress>,
    val term: Int,
    val raftLog: CellRef,
    val replicator: CellRef,
    val election: CellRef,
    sender: CellRef
) : CellEvent(sender) {
    fun reply(address: NodeAddress) {
        reply(EnablePeerLogReplicatorReplyEvent(address))
    }
}

class EnablePeerLogReplicatorReplyEvent(val address: NodeAddress): Event

class DisablePeerLogReplicatorCellEvent(sender: CellRef) : CellEvent(sender) {
    fun reply(addressSet: Set<NodeAddress>, unavailable: List<NodeAddress>) {
        reply(DisablePeerLogReplicatorReplyEvent(addressSet, unavailable))
    }
}

class DisablePeerLogReplicatorReplyEvent(val addressSet: Set<NodeAddress>, val unavailable: List<NodeAddress>): Event

class PeerLogReplicatorDisabledEvent(val address: NodeAddress): Event

internal class PeerMessageForwardEvent(val message: PeerMessage) : Event
internal class ConnectionClosedEvent(val address: NodeAddress) : Event