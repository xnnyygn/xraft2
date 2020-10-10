package `in`.xnnyygn.xraft2

import `in`.xnnyygn.xraft2.cell.Cell
import `in`.xnnyygn.xraft2.cell.CellContext
import `in`.xnnyygn.xraft2.cell.Event
import `in`.xnnyygn.xraft2.log.NewPeerLogReplicationDoneEvent
import `in`.xnnyygn.xraft2.net.ConnectionClosedEvent
import `in`.xnnyygn.xraft2.net.NodeAddress

class NewPeerTask(private val address: NodeAddress) : Cell() {
    override fun receive(context: CellContext, event: Event) {
        when (event) {
            is NameDuplicatedEvent -> {
            }
            is NewPeerLogReplicationDoneEvent -> {
            }
            is ConnectionClosedEvent -> {
            }
        }
    }
}