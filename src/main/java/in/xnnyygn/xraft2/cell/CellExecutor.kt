package `in`.xnnyygn.xraft2.cell

import java.util.concurrent.ExecutorService
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit

sealed class CellExecutor(
    private val cell: Cell
) : CellRef, CellContext, CellTaskExecutor {

    private val queue = CellQueue<Message>()
    private val children = mutableListOf<ChildCellExecutor>()

    // TODO merge started and stopped to one variable of type int
    @Volatile
    private var started = false

    @Volatile
    private var stopping: Boolean = false

    open fun start() {
        started = true
        send(CellStartMessage)
    }

    override fun send(msg: Message) {
        if (!started) {
            throw IllegalStateException("not started")
        }
        if (queue.offerAndCount(msg) > 1) {
            // cell is running
            return
        }
        submit(CellTask(cell, this, queue, this))
    }

    override fun startChild(cell: Cell): CellRef {
        val child = ChildCellExecutor(cell, this)
        children.add(child)
        child.start()
        return child
    }

    override fun schedule(time: Long, unit: TimeUnit, msg: Message): ScheduledFuture<*> {
        return schedule({ send(msg) }, time, unit)
    }

    abstract fun schedule(action: () -> Unit, time: Long, unit: TimeUnit): ScheduledFuture<*>

    override fun stopSelf() {
        stop()
    }

    open fun stop() {
        stopping = true
        if (children.isNotEmpty()) {
            for (child in children) {
                child.stop()
            }
            children.clear()
        }
        send(CellStopMessage)
    }

    fun removeChild(child: CellExecutor) {
        if (!stopping) {
            children.remove(child)
        }
    }
}

class RootCellExecutor(
    cell: Cell,
    private val executorService: ExecutorService,
    private val scheduledExecutorService: ScheduledExecutorService
) : CellExecutor(cell) {

    override val parent: CellRef
        get() = EmptyCellRef

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