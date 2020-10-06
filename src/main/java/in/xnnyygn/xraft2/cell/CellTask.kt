package `in`.xnnyygn.xraft2.cell

import java.util.*

class CellTask(
    private val cell: Cell,
    private val context: CellContext,
    private val queue: CellQueue<Message>
) {
    enum class NextState {
        NO, YES;
    }

    fun run(): NextState {
        when (val next = queue.peek()) {
            null -> throw IllegalStateException("no next message")
            CellStartMessage -> cell.start(context)
            CellStopMessage -> cell.stop(context)
            else -> cell.receive(context, next)
        }
        return if (queue.removeAndCount() == 0) {
            NextState.NO
        } else {
            NextState.YES
        }
    }
}

internal object CellStartMessage : Message
internal object CellStopMessage : Message