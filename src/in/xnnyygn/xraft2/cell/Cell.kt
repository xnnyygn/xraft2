package `in`.xnnyygn.xraft2.cell

import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit

abstract class Cell {
    open fun start(context: CellContext) {}

    abstract fun receive(context: CellContext, msg: Message)

    open fun stop(context: CellContext) {}
}

interface CellRef {
    fun send(msg: Message)
}

class CellContext(val parent: CellRef) {
    fun startChild(cell: Cell): CellRef {
        TODO()
    }

    fun schedule(time: Long, unit: TimeUnit, msg: Message): ScheduledFuture<Unit> {
        TODO()
    }
}