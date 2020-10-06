package `in`.xnnyygn.xraft2.net

import `in`.xnnyygn.xraft2.cell.Cell
import `in`.xnnyygn.xraft2.cell.CellContext
import `in`.xnnyygn.xraft2.cell.CellRef
import `in`.xnnyygn.xraft2.cell.Message

class AcceptorCell(private val connections: CellRef) : Cell() {
    override fun start(context: CellContext) {
        context.parent.send(AcceptorInitializedMessage)
    }

    override fun receive(context: CellContext, msg: Message) {
        // TODO stop acceptor
    }
}

object AcceptorInitializedMessage : Message