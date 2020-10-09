package `in`.xnnyygn.xraft2.net

import `in`.xnnyygn.xraft2.FixedThreadFactory
import `in`.xnnyygn.xraft2.cell.Cell
import `in`.xnnyygn.xraft2.cell.CellContext
import `in`.xnnyygn.xraft2.cell.CellRef
import `in`.xnnyygn.xraft2.cell.CellEvent
import `in`.xnnyygn.xraft2.getLogger
import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.Channel
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInboundHandlerAdapter
import io.netty.channel.ChannelInitializer
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.nio.NioServerSocketChannel

class ServerCell(
    private val address: NodeAddress,
    private val workerGroup: NioEventLoopGroup,
    private val connectionPool: CellRef
) : Cell() {
    private val bossGroup = NioEventLoopGroup(1, FixedThreadFactory("acceptor"))

    override fun start(context: CellContext) {
        val serverBootstrap = ServerBootstrap()
            .group(bossGroup, workerGroup)
            .channel(NioServerSocketChannel::class.java)
            .childHandler(object : ChannelInitializer<NioServerSocketChannel>() {
                override fun initChannel(ch: NioServerSocketChannel) {
                    val pipeline = ch.pipeline()
                    pipeline.addLast(IncomingHandshakeHandler(address.name, connectionPool))
                }
            })
        context.logger.info { "listen at ${address.port}" }
        serverBootstrap.bind(address.port).addListener { f ->
            if (f.isSuccess) {
                context.parent.tell(ServerInitializedEvent)
            } else {
                context.logger.warn("failed to bind port", f.cause())
                // parent will stop this acceptor
                context.parent.tell(ServerInitializationFailedEvent)
            }
        }
    }

    override fun receive(context: CellContext, event: CellEvent) {
    }

    override fun stop(context: CellContext) {
        context.logger.debug("shutdown event loop group")
        bossGroup.shutdownGracefully().sync()
    }
}

internal class IncomingHandshakeHandler(
    private val name: String,
    private val connectionPool: CellRef
) : ChannelInboundHandlerAdapter() {
    companion object {
        val logger = getLogger(IncomingHandshakeHandler::class.java)
    }

    private var handshake = false

    override fun channelRead(ctx: ChannelHandlerContext, msg: Any) {
        if (msg is HandshakeRpc) {
            if (handshake) {
                logger.warn("unexpected name from remote, handshake is already done")
                ctx.close()
            } else {
                connectionPool.tell(IncomingChannelEvent(msg.name, ctx.channel()))
            }
        } else {
            super.channelRead(ctx, msg)
        }
    }

    inner class IncomingChannelEvent(
        val remoteName: String,
        val channel: Channel
    ) : CellEvent {
        fun reply() {
            handshake = true
            channel.write(HandshakeReply(name))
        }
    }
}

object ServerInitializedEvent : CellEvent
object ServerInitializationFailedEvent : CellEvent