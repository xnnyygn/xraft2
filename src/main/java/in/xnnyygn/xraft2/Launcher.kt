package `in`.xnnyygn.xraft2

import `in`.xnnyygn.xraft2.cell.CellSystem
import io.netty.channel.nio.NioEventLoopGroup

fun main(args: Array<String>) {
    val system = CellSystem()
    val workerGroup = NioEventLoopGroup(NumberedThreadFactory("io-"))
    system.add(InitializerCell(workerGroup))
    system.start()
    System.`in`.read()
    system.stop()
    workerGroup.shutdownGracefully().sync()
}