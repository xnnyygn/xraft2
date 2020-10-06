package `in`.xnnyygn.xraft2.log

import `in`.xnnyygn.xraft2.cell.Cell
import `in`.xnnyygn.xraft2.cell.CellContext
import `in`.xnnyygn.xraft2.cell.Message

class ReadIndexTask: Cell() {
    override fun receive(context: CellContext, msg: Message) {
        TODO("Not yet implemented")
    }
}