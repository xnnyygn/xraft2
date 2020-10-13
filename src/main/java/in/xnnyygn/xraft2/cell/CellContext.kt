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

    fun suspendBy(cell: Cell)

    fun suspendBy(cell: CellRef)

    fun resumeParent()

    fun resumeParent(result: Event)

    fun resume(cell: CellRef)

    fun resume(cell: CellRef, result: Event)

    fun schedule(time: Long, unit: TimeUnit, event: Event): ScheduledFuture<*>

    fun findCell(path: String): CellRef

    fun stopSelf()
}