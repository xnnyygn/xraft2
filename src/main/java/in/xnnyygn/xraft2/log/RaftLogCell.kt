package `in`.xnnyygn.xraft2.log

import `in`.xnnyygn.xraft2.cell.Cell
import `in`.xnnyygn.xraft2.cell.CellContext
import `in`.xnnyygn.xraft2.cell.CellRef
import `in`.xnnyygn.xraft2.cell.Message

class RaftLogCell(private val connections: CellRef) : Cell() {
    private var _raftLog: RaftLog? = null

    private val raftLog: RaftLog
        get() = _raftLog!!

    override fun start(context: CellContext) {
        val logReplicator = context.startChild(LogReplicatorCell(connections))
        // sync
        _raftLog = RaftLog(EmptyLogSequence, EmptySnapshot, logReplicator)
        context.parent.send(LogInitializedMessage)
    }

    override fun receive(context: CellContext, msg: Message) {
        TODO("Not yet implemented")
    }
}

/**
 * from [in.xnnyygn.xraft2.InitializerCell]
 */
object LogInitializedMessage : Message