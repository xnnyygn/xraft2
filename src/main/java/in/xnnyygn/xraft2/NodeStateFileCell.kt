package `in`.xnnyygn.xraft2

import `in`.xnnyygn.xraft2.cell.Cell
import `in`.xnnyygn.xraft2.cell.CellContext
import `in`.xnnyygn.xraft2.cell.Message

class NodeStateFileCell: Cell() {
    override fun start(context: CellContext) {
        // TODO load node state from file
        context.parent.send(NodeStateLoadedMessage(NodeState(1, null)))
    }

    override fun receive(context: CellContext, msg: Message) {
        // TODO save node state to file
    }
}

data class NodeStateLoadedMessage(val state: NodeState): Message