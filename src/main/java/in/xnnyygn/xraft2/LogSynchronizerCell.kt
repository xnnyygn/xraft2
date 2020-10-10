package `in`.xnnyygn.xraft2

import `in`.xnnyygn.xraft2.cell.Cell
import `in`.xnnyygn.xraft2.cell.CellContext
import `in`.xnnyygn.xraft2.cell.CellRef
import `in`.xnnyygn.xraft2.cell.Event

class LogSynchronizerCell(
    private val election: CellRef,
    private val raftLog: CellRef,
    private val serverList: CellRef
) : Cell() {
    override fun receive(context: CellContext, event: Event) {
        TODO("Not yet implemented")
    }
}