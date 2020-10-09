package `in`.xnnyygn.xraft2.cell

import `in`.xnnyygn.xraft2.Logger
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit

// thread safe
interface CellContext {
    val self: CellRef

    val parent: CellRef

    val logger: Logger

    fun startChild(cell: Cell): CellRef

    fun schedule(time: Long, unit: TimeUnit, event: CellEvent): ScheduledFuture<*>

    fun stopSelf()
}