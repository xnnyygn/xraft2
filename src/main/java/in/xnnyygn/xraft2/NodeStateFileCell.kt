package `in`.xnnyygn.xraft2

import `in`.xnnyygn.xraft2.cell.Cell
import `in`.xnnyygn.xraft2.cell.CellContext
import `in`.xnnyygn.xraft2.cell.Event

class NodeStateFileCell: Cell() {
    override fun start(context: CellContext) {
        // TODO load node state from file
        context.parent.tell(NodeStateLoadedEvent(NodeState(1, null)))
    }

    override fun receive(context: CellContext, event: Event) {
        // TODO save node state to file
    }
}

data class NodeStateLoadedEvent(val state: NodeState): Event