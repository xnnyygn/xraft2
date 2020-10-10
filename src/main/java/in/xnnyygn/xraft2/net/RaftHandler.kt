package `in`.xnnyygn.xraft2.net

import `in`.xnnyygn.xraft2.cell.CellContext
import `in`.xnnyygn.xraft2.getLogger
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInboundHandlerAdapter

internal class RaftHandler(
    private val address: NodeAddress,
    context: CellContext // context of connection cell
) : ChannelInboundHandlerAdapter() {
    companion object {
        val logger = getLogger(RaftHandler::class.java)
    }

    private val election = context.findCell("/Election")
    private val logSynchronizer = context.findCell("/LogSynchronizer")
    private val connection = context.self

    override fun exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
        logger.warn("io error occurred", cause)
        ctx.close()
    }

    override fun channelRead(ctx: ChannelHandlerContext, msg: Any) {
        when (msg) {
            is RequestVoteRpc -> election.tell(ConnectionMessageEvent(msg, connection))
            is RequestVoteReply -> election.tell(ConnectionMessageEvent(msg, connection))
            is AppendEntriesRpc -> logSynchronizer.tell(ConnectionMessageEvent(msg, connection))
            is AppendEntriesReply -> connection.tell(PeerMessageForwardEvent(msg))
            else -> logger.warn { "unexpected message from $address: $msg" }
        }
    }
}