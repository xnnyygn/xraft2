package `in`.xnnyygn.xraft2.cell

abstract class Cell {
    open val name: String = javaClass.simpleName

    open fun start(context: CellContext) {}

    abstract fun receive(context: CellContext, event: CellEvent)

    open fun stop(context: CellContext) {}
}