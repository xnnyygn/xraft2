package `in`.xnnyygn.xraft2.net

import `in`.xnnyygn.xraft2.cell.Cell
import `in`.xnnyygn.xraft2.cell.CellContext
import `in`.xnnyygn.xraft2.cell.CellRef
import `in`.xnnyygn.xraft2.cell.Event
import `in`.xnnyygn.xraft2.getLogger
import io.netty.bootstrap.Bootstrap
import io.netty.channel.*
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.nio.NioSocketChannel

class ClientCell(
    private val sourceName: String,
    private val destination: NodeAddress,
    private val workerGroup: NioEventLoopGroup
) : Cell() {
    private var done = false
    private var channelFuture: ChannelFuture? = null

    override val name: String = "Client(${destination})"

    override fun start(context: CellContext) {
        val bootstrap: Bootstrap = Bootstrap()
            .group(workerGroup)
            .channel(NioSocketChannel::class.java)
            .option(ChannelOption.TCP_NODELAY, true)
            .handler(object : ChannelInitializer<NioSocketChannel>() {
                @Throws(Exception::class)
                override fun initChannel(ch: NioSocketChannel) {
                    val pipeline: ChannelPipeline = ch.pipeline()
                    pipeline.addLast(OutgoingHandshakeHandler(sourceName, destination, context.self))
                }
            })
        channelFuture = bootstrap
            .connect(destination.ip, destination.port)
            .addListener { f ->
                if (f.isSuccess) {
                    return@addListener
                }
                context.logger.warn(f.cause()) { "failed to connect ${destination.ip}:${destination.port}" }
                context.parent.tell(ClientConnectionFailedEvent(destination))
            }
    }

    override fun receive(context: CellContext, event: Event) {
        if (event is OutgoingChannelEvent) {
            done = true
            context.logger.info("handshake successfully $sourceName -> ${destination.name}")
            event.channel.pipeline().removeLast()
            context.parent.tell(event)
            context.stopSelf()
        }
    }

    override fun stop(context: CellContext) {
        if (done) return

        val f = this.channelFuture!!
        if (f.isCancellable) {
            f.cancel(true)
            f.await()
        }
    }
}

internal class OutgoingHandshakeHandler(
    private val sourceName: String,
    private val destination: NodeAddress,
    private val client: CellRef
) : ChannelInboundHandlerAdapter() {
    companion object {
        val logger = getLogger(OutgoingHandshakeHandler::class.java)
    }

    private var handshake = false

    override fun channelActive(ctx: ChannelHandlerContext) {
        ctx.channel().write(HandshakeRpc(sourceName))
        super.channelActive(ctx)
    }

    override fun channelRead(ctx: ChannelHandlerContext, msg: Any) {
        if (msg !is HandshakeReply) {
            super.channelRead(ctx, msg)
            return
        }
        when {
            handshake -> {
                logger.warn { "duplicated handshake reply from ${ctx.channel().remoteAddress()}" }
                ctx.close()
            }
            msg.name != destination.name -> {
                logger.warn {
                    "unexpected name from ${
                        ctx.channel().remoteAddress()
                    }, expected ${destination.name}, but was ${msg.name}"
                }
                ctx.close()
            }
            else -> {
                handshake = true
                client.tell(OutgoingChannelEvent(ctx.channel(), destination))
            }
        }
    }
}

internal class OutgoingChannelEvent(val channel: Channel, val address: NodeAddress) : Event
internal class ClientConnectionFailedEvent(val address: NodeAddress) : Event