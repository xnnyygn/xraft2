package `in`.xnnyygn.xraft2.cell

import `in`.xnnyygn.xraft2.Logger
import `in`.xnnyygn.xraft2.getLogger
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicInteger

sealed class CellExecutor(
    protected val cell: Cell
) : CellRef, CellContext, CellTaskExecutor {

    companion object {
        protected val logger = getLogger(CellExecutor::class.java)

        private const val STATUS_NOT_STARTED = 0
        private const val STATUS_STARTING_OR_STARTED = 1
        private const val STATUS_STOPPING_OR_STOPPED = 2
    }

    private val queue = CellQueue<Message>()
    private val _status = AtomicInteger(STATUS_NOT_STARTED)
    private val childSet = CellChildSet()

    private val status: Int
        get() = _status.get()

    val name: String
        get() = cell.name

    abstract val fullName: String

    override val logger: Logger
        get() = CellExecutor.logger

    open fun start() {
        updateStatus(STATUS_NOT_STARTED, STATUS_STARTING_OR_STARTED)
        logger.debug { "cell $fullName: start" }
        submit(CellStartMessage)
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

    private fun submit(msg: Message) {
        if (queue.offerAndCount(msg) > 1) {
            // cell is running
            return
        }
        submit(CellTask(cell, this, queue, this))
    }

    override fun send(msg: Message) {
        // cannot send message to a cell when it is not started or it is stopping or already stopped
        ensureStatus(STATUS_STARTING_OR_STARTED)
        logger.debug("cell $fullName: message $msg")
        submit(msg)
    }

    override fun startChild(cell: Cell): CellRef {
        val child = ChildCellExecutor(cell, this)
        childSet.add(child)
        logger.debug { "cell $fullName: add child ${cell.name}" }
        child.start()
        return child
    }

    override fun schedule(time: Long, unit: TimeUnit, msg: Message): ScheduledFuture<*> {
        logger.debug { "cell $fullName: schedule $msg after $time $unit" }
        return schedule({ send(msg) }, time, unit)
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
        logger.debug { "cell $fullName: stop" }
        updateStatus(STATUS_STARTING_OR_STARTED, STATUS_STOPPING_OR_STOPPED)
        childSet.stopAllAndAwait()
        submit(CellStopMessage)
    }

    fun removeChild(child: CellExecutor) {
        logger.debug { "cell $fullName: remove child ${child.name}" }
        childSet.remove(child)
    }
}

class RootCellExecutor(
    cell: Cell,
    private val executorService: ExecutorService,
    private val scheduledExecutorService: ScheduledExecutorService
) : CellExecutor(cell) {

    override val parent: CellRef
        get() = EmptyCellRef

    override val fullName: String
        get() = ('/' + cell.name)

    override fun submit(task: CellTask) {
        executorService.submit(task)
    }

    override fun schedule(action: () -> Unit, time: Long, unit: TimeUnit): ScheduledFuture<*> {
        return scheduledExecutorService.schedule(action, time, unit)
    }
}

class ChildCellExecutor(
    cell: Cell,
    override val parent: CellExecutor
) : CellExecutor(cell) {

    override val fullName: String
        get() = (parent.fullName + '/' + cell.name)

    override fun submit(task: CellTask) {
        parent.submit(task)
    }

    override fun schedule(action: () -> Unit, time: Long, unit: TimeUnit): ScheduledFuture<*> {
        return parent.schedule(action, time, unit)
    }

    override fun stop() {
        super.stop()
        parent.removeChild(this)
    }
}