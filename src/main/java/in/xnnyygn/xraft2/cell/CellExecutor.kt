package `in`.xnnyygn.xraft2.cell

import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit

sealed class CellExecutor(
    private val cell: Cell
) : CellRef, CellContext {
    private val queue = CellQueue<Message>()
    private val children = mutableListOf<ChildCellExecutor>()

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
        submit(CellTask(cell, this, queue))
    }

    override fun startChild(cell: Cell): CellRef {
        val child = ChildCellExecutor(cell, this)
        children.add(child)
        child.start()
        return child
    }

    abstract fun submit(task: CellTask)

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
    private val workerGroup: CellWorkerGroup,
    private val scheduledExecutorService: ScheduledExecutorService
) : CellExecutor(cell) {

    override val parent: CellRef
        get() = EmptyCellRef

    override fun submit(task: CellTask) {
        workerGroup.submit(task)
    }

    override fun schedule(time: Long, unit: TimeUnit, msg: Message): ScheduledFuture<*> {
        return scheduledExecutorService.schedule({ send(msg) }, time, unit)
    }
}

class ChildCellExecutor(
    cell: Cell,
    override val parent: CellExecutor
) : CellExecutor(cell) {

    override fun submit(task: CellTask) {
        parent.submit(task)
    }

    override fun schedule(time: Long, unit: TimeUnit, msg: Message): ScheduledFuture<*> {
        return parent.schedule(time, unit, msg)
    }

    override fun stop() {
        super.stop()
        parent.removeChild(this)
    }
}