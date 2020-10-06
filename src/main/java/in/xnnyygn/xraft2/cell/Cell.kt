package `in`.xnnyygn.xraft2.cell

abstract class Cell {
    open fun start(context: CellContext) {}

    abstract fun receive(context: CellContext, msg: Message)

    open fun stop(context: CellContext) {}
}