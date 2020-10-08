package `in`.xnnyygn.xraft2.cell

import `in`.xnnyygn.xraft2.Logger
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit

interface CellContext {
    val parent: CellRef

    // TODO remove me
    val logger: Logger

    fun startChild(cell: Cell): CellRef

    fun schedule(time: Long, unit: TimeUnit, msg: Message): ScheduledFuture<*>

    fun stopSelf()
}