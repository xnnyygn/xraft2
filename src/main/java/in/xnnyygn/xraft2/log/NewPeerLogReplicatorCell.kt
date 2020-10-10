package `in`.xnnyygn.xraft2.log

import `in`.xnnyygn.xraft2.cell.*
import `in`.xnnyygn.xraft2.getLogger
import `in`.xnnyygn.xraft2.log.NewPeerLogReplicationHandler
import `in`.xnnyygn.xraft2.net.AppendEntriesReply
import `in`.xnnyygn.xraft2.net.NodeAddress
import `in`.xnnyygn.xraft2.net.PeerMessageEvent
import io.netty.channel.Channel
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInboundHandlerAdapter

class NewPeerLogReplicatorCell(
    private val address: NodeAddress,
    private val channel: Channel
) : Cell() {
    private var done = false

    override val name: String = "NewPeerLogReplicatorCell($address)"

    override fun start(context: CellContext) {
        channel.pipeline().addLast(NewPeerLogReplicationHandler(address, context.self))
    }

    override fun receive(context: CellContext, event: Event) {
        if (event == PoisonPill) {
            done = true
            channel.pipeline().removeLast()
            context.parent.tell(NewPeerLogReplicationDoneEvent(address))
            context.stopSelf()
        }
    }

    override fun stop(context: CellContext) {
        if (!done && channel.isOpen) {
            channel.close()
        }
    }
}

internal class NewPeerLogReplicationHandler(
    private val address: NodeAddress,
    private val replicator: CellRef,
) : ChannelInboundHandlerAdapter() {
    companion object {
        val logger = getLogger(NewPeerLogReplicationHandler::class.java)
    }

    override fun exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
        logger.warn("io error occurred", cause)
        ctx.close()
    }

    override fun channelRead(ctx: ChannelHandlerContext, msg: Any) {
        if (msg is AppendEntriesReply) {
            replicator.tell(PeerMessageEvent(msg))
        } else {
            logger.warn { "unexpected message from $address: $msg" }
        }
    }
}

class NewPeerLogReplicationDoneEvent(val address: NodeAddress) : Event
