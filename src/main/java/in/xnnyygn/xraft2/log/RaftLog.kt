package `in`.xnnyygn.xraft2.log

import `in`.xnnyygn.xraft2.cell.CellRef

class RaftLog(
    private val logSequence: LogSequence,
    private val snapshot: Snapshot,
    private val logReplicator: CellRef
) {

}

