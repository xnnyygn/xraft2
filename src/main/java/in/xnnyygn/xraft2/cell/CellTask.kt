package `in`.xnnyygn.xraft2.cell

class CellTask(
    private val cell: Cell,
    private val context: CellContext,
    private val queue: CellQueue<Event>,
    private val executor: CellExecutor
) : Runnable {
    override fun run() {
        when (val next = queue.peek()) {
            null -> throw IllegalStateException("no next message")
            InternalStartEvent -> start()
            InternalStopEvent -> stop()
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

    private fun receive(event: Event) {
        try {
            cell.receive(context, event)
        } catch (t: Throwable) {
            context.logger.warn(t) { "failed to execute with message $event" }
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

internal object InternalStartEvent : Event
internal object InternalStopEvent : Event