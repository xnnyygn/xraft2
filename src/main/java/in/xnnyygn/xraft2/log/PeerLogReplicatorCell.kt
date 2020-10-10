package `in`.xnnyygn.xraft2.log

import `in`.xnnyygn.xraft2.cell.Cell
import `in`.xnnyygn.xraft2.cell.CellContext
import `in`.xnnyygn.xraft2.cell.Event
import `in`.xnnyygn.xraft2.net.NodeAddress
import io.netty.channel.Channel

class PeerLogReplicatorCell(
    private val address: NodeAddress,
    private val channel: Channel) : Cell() {
    override fun receive(context: CellContext, event: Event) {
        TODO("Not yet implemented")
    }
}