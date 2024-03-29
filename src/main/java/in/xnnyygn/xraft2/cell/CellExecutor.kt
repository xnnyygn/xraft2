package `in`.xnnyygn.xraft2.cell

import `in`.xnnyygn.xraft2.Logger
import java.util.concurrent.ExecutorService
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

sealed class CellExecutor(
    protected val cell: Cell
) : CellRef, CellContext, CellTaskExecutor {

    companion object {
        private const val STATUS_NOT_STARTED = 0
        private const val STATUS_STARTING_OR_STARTED = 1
        private const val STATUS_STOPPING_OR_STOPPED = 2
    }

    private val queue = CellQueue()
    private val _status = AtomicInteger(STATUS_NOT_STARTED)
    private val childSet = CellChildSet()

    private val status: Int
        get() = _status.get()

    override val self: CellRef
        get() = this

    private val name: String = cell.name

    abstract val fullName: String

    open fun start() {
        updateStatus(STATUS_NOT_STARTED, STATUS_STARTING_OR_STARTED)
        logger.debug("start")
        submit(InternalStartEvent)
    }

    private fun updateStatus(expected: Int, newStatus: Int) {
        ensureStatus(expected)
        // expected != updated
        if (!_status.compareAndSet(expected, newStatus)) {
            // if two or more threads try to start or stop a cell at the same time,
            // only one thread will win and others will get an exception
            throw IllegalStateException("failed to update status from $expected to $newStatus")
        }
    }

    private fun ensureStatus(expected: Int) {
        val actual = _status.get()
        if (actual != expected) {
            throw IllegalStateException("unexpected cell status, expect $expected, but was $actual")
        }
    }

    fun submit(event: Event) {
        if (queue.offerAndCanRun(event)) {
            submit(CellTask(cell, this, queue, this))
        }
    }

    override fun tell(event: Event) {
        // cannot send message to a cell when it is not started or it is stopping or already stopped
        ensureStatus(STATUS_STARTING_OR_STARTED)
        logger.debug("message $event")
        submit(event)
    }

    // TODO rename cell to child
    override fun startChild(cell: Cell): CellRef {
        // TODO add method newExecutor
        val child = ChildCellExecutor(cell, this)
        childSet.add(child)
        logger.debug { "add child ${cell.name}" }
        child.start()
        return child
    }

    override fun suspendBy(cell: Cell) {
        val c = ChildCellExecutor(cell, this)
        childSet.add(c)
        queue.suspendBy(c)
        logger.debug { "add child ${cell.name}" }
        c.start()
    }

    override fun suspendBy(cell: CellRef) {
        queue.suspendBy(cell)
    }

    override fun resume(cell: CellRef) {
        resume(cell, VoidEvent)
    }

    override fun resume(cell: CellRef, result: Event) {
        submit(ResumeEvent(result, cell))
    }

    override fun findCell(path: String): CellRef {
        TODO("Not yet implemented")
    }

    override fun schedule(time: Long, unit: TimeUnit, event: Event): ScheduledFuture<*> {
        logger.debug { "schedule $event after $time $unit" }
        return schedule({ tell(event) }, time, unit)
    }

    abstract fun schedule(action: () -> Unit, time: Long, unit: TimeUnit): ScheduledFuture<*>

    override fun stopSelf() {
        stop()
    }

    open fun stop() {
        /**
         * if a child is stopping and its parent is stopping at the same time,
         * stop method of the child might be called twice
         */
        if (status == STATUS_STOPPING_OR_STOPPED) {
            return
        }
        logger.debug("stop")
        updateStatus(STATUS_STARTING_OR_STARTED, STATUS_STOPPING_OR_STOPPED)
        childSet.stopAllAndAwait()
        submit(InternalStopEvent)
    }

    fun removeChild(child: CellExecutor) {
        logger.debug { "remove child ${child.name}" }
        childSet.remove(child)
    }

    abstract fun removeSelfFromParent()

    override fun toString(): String {
        return "CellExecutor($fullName)"
    }
}

class RootCellExecutor(
    cell: Cell,
    private val executorService: ExecutorService,
    private val scheduledExecutorService: ScheduledExecutorService
) : CellExecutor(cell) {

    override val parent: CellRef = EmptyCellRef

    override val fullName = ('/' + cell.name)

    override val logger: Logger = CellLogger("cell://${cell.name}", cell.javaClass)

    override fun submit(task: CellTask) {
        executorService.submit(task)
    }

    override fun resumeParent() {
    }

    override fun resumeParent(result: Event) {
    }

    override fun schedule(action: () -> Unit, time: Long, unit: TimeUnit): ScheduledFuture<*> {
        return scheduledExecutorService.schedule(action, time, unit)
    }

    override fun removeSelfFromParent() {
    }
}

class ChildCellExecutor(
    cell: Cell,
    override val parent: CellExecutor
) : CellExecutor(cell) {

    override val fullName = (parent.fullName + '/' + cell.name)

    override val logger: Logger = CellLogger("cell:/$fullName", cell.javaClass)

    override fun submit(task: CellTask) {
        parent.submit(task)
    }

    override fun resumeParent() {
        resumeParent(VoidEvent)
    }

    override fun resumeParent(result: Event) {
        parent.submit(ResumeEvent(result, this))
    }

    override fun schedule(action: () -> Unit, time: Long, unit: TimeUnit): ScheduledFuture<*> {
        return parent.schedule(action, time, unit)
    }

    override fun removeSelfFromParent() {
        parent.removeChild(this)
    }
}