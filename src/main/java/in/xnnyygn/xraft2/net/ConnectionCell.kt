package `in`.xnnyygn.xraft2.net

import `in`.xnnyygn.xraft2.cell.Cell
import `in`.xnnyygn.xraft2.cell.CellContext
import `in`.xnnyygn.xraft2.cell.CellEvent
import `in`.xnnyygn.xraft2.cell.PoisonPill
import io.netty.channel.Channel

class ConnectionCell(
    private val address: NodeAddress,
    private val channel: Channel
) : Cell() {
    override val name: String = "Connection<${address}>"

    override fun start(context: CellContext) {
        channel.closeFuture().addListener {
            context.logger.info("connection closed")
            context.parent.tell(ConnectionClosedEvent(address))
        }
//        channel.pipeline().addLast()
    }

    override fun receive(context: CellContext, event: CellEvent) {
        when (event) {
            is PendingMessagesEvent -> channel.write(event.queue.last()) // queue won't be empty
            is PeerMessageEvent -> channel.write(event.message)
            is PoisonPill -> context.stopSelf()
        }
    }

    override fun stop(context: CellContext) {
        if (channel.isOpen) {
            channel.close()
        }
    }
}

class PeerMessageEvent(val message: PeerMessage) : CellEvent

internal class PendingMessagesEvent(val queue: List<PeerMessage>) : CellEvent
internal class ConnectionClosedEvent(val address: NodeAddress) : CellEvent