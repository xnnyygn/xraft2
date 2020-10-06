package `in`.xnnyygn.xraft2.cell

interface CellRef {
    fun send(msg: Message)
}

object EmptyCellRef : CellRef {
    override fun send(msg: Message) {
    }
}