package `in`.xnnyygn.xraft2.cell

class CellTask(
    private val cell: Cell,
    private val context: CellContext,
    private val queue: CellQueue<Message>,
    private val executor: CellTaskExecutor
) : Runnable {
    override fun run() {
        when (val next = queue.peek()) {
            null -> throw IllegalStateException("no next message")
            CellStartMessage -> cell.start(context)
            CellStopMessage -> cell.stop(context)
            else -> cell.receive(context, next)
        }
        if (queue.removeAndCount() > 0) {
            executor.submit(this)
        }
    }
}

interface CellTaskExecutor {
    fun submit(task: CellTask)
}

internal object CellStartMessage : Message
internal object CellStopMessage : Message