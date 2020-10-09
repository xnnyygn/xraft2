package `in`.xnnyygn.xraft2.cell

class CellTask(
    private val cell: Cell,
    private val context: CellContext,
    private val queue: CellQueue<Message>,
    private val executor: CellExecutor
) : Runnable {
    override fun run() {
        when (val next = queue.peek()) {
            null -> throw IllegalStateException("no next message")
            CellStartMessage -> start()
            CellStopMessage -> stop()
            else -> receive(next)
        }
    }

    private fun start() {
        try {
            cell.start(context)
        } catch (t: Throwable) {
            context.logger.warn("failed to start", t)
            // TODO queue -> DLQ
            executor.removeSelfFromParent()
            return
        }
        submitIfHasMoreMessage()
    }

    private fun submitIfHasMoreMessage() {
        if (queue.removeAndCount() > 0) {
            executor.submit(this)
        }
    }

    private fun receive(msg: Message) {
        try {
            cell.receive(context, msg)
        } catch (t: Throwable) {
            context.logger.warn(t) { "failed to execute with message $msg" }
            // TODO restart
            executor.removeSelfFromParent()
            return
        }
        submitIfHasMoreMessage()
    }

    private fun stop() {
        try {
            cell.stop(context)
        } catch (t: Throwable) {
            context.logger.warn(t) { "failed to stop" }
        }
        if (queue.removeAndCount() != 0) {
            throw IllegalStateException("illegal new message while stopping")
        }
        executor.removeSelfFromParent()
    }
}

interface CellTaskExecutor {
    fun submit(task: CellTask)
}

internal object CellStartMessage : Message
internal object CellStopMessage : Message