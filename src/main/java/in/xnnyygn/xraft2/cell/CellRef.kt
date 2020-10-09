package `in`.xnnyygn.xraft2.cell

interface CellRef {
    fun tell(event: CellEvent)
}

object EmptyCellRef : CellRef {
    override fun tell(event: CellEvent) {
    }
}