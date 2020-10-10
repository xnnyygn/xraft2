package `in`.xnnyygn.xraft2.cell

interface CellRef {
    fun tell(event: Event)
}

object EmptyCellRef : CellRef {
    override fun tell(event: Event) {
    }
}