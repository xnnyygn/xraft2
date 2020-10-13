package `in`.xnnyygn.xraft2.cell

class CellTask(
    private val cell: Cell,
    private val context: CellContext,
    private val queue: CellQueue,
    private val executor: CellExecutor
) : Runnable {
    override fun run() {
        when (val next = queue.poll()) {
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
        if (queue.hasNext()) {
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
        if (queue.hasNext()) {
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

internal object VoidEvent: Event
internal class ResumeEvent(val result: Event, val cell: CellRef): Event