package `in`.xnnyygn.xraft2.cell

import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit

interface CellContext {
    val parent: CellRef

    fun startChild(cell: Cell): CellRef

    fun schedule(time: Long, unit: TimeUnit, msg: Message): ScheduledFuture<*>
}