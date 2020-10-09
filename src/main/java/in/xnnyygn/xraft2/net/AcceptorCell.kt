package `in`.xnnyygn.xraft2.net

import `in`.xnnyygn.xraft2.FixedThreadFactory
import `in`.xnnyygn.xraft2.NumberedThreadFactory
import `in`.xnnyygn.xraft2.cell.Cell
import `in`.xnnyygn.xraft2.cell.CellContext
import `in`.xnnyygn.xraft2.cell.CellRef
import `in`.xnnyygn.xraft2.cell.Message
import `in`.xnnyygn.xraft2.getLogger
import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.Channel
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInboundHandlerAdapter
import io.netty.channel.ChannelInitializer
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.channel.socket.nio.NioSocketChannel

class AcceptorCell(
    private val port: Int,
    private val connections: CellRef
) : Cell() {
    private val bossGroup = NioEventLoopGroup(1, FixedThreadFactory("acceptor"))
    private val workerGroup = NioEventLoopGroup(NumberedThreadFactory("io-"))

    override fun start(context: CellContext) {
        val serverBootstrap = ServerBootstrap()
            .group(bossGroup, workerGroup)
            .channel(NioServerSocketChannel::class.java)
            .childHandler(object : ChannelInitializer<NioSocketChannel>() {
                override fun initChannel(ch: NioSocketChannel?) {
                    val pipeline = ch!!.pipeline()
                    pipeline.addLast(IncomingHandler(connections))
                }
            });
        context.logger.info { "listen at $port" }
        serverBootstrap.bind(port).addListener { f ->
            if (f.isSuccess) {
                context.parent.send(AcceptorInitializedMessage)
            } else {
                context.logger.warn("failed to bind port", f.cause())
                // parent will stop acceptor
                context.parent.send(AcceptorInitializationFailedMessage)
            }
        }
    }

    override fun receive(context: CellContext, msg: Message) {
    }

    override fun stop(context: CellContext) {
        context.logger.debug("shutdown event loop group")
        workerGroup.shutdownGracefully().sync()
        bossGroup.shutdownGracefully().sync()
    }
}

internal class IncomingHandler(private val connections: CellRef) : ChannelInboundHandlerAdapter() {
    companion object {
        val logger = getLogger(IncomingHandler::class.java)
    }

    override fun exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
        logger.warn("error occurred", cause)
        ctx.close()
    }

    override fun channelActive(ctx: ChannelHandlerContext) {
        connections.send(IncomingChannelMessage(ctx.channel()))
    }
}

object AcceptorInitializedMessage : Message
object AcceptorInitializationFailedMessage : Message
class IncomingChannelMessage(val channel: Channel) : Message