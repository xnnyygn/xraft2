package `in`.xnnyygn.xraft2.log

import `in`.xnnyygn.xraft2.cell.Cell
import `in`.xnnyygn.xraft2.cell.CellContext
import `in`.xnnyygn.xraft2.cell.CellRef
import `in`.xnnyygn.xraft2.cell.CellEvent
import `in`.xnnyygn.xraft2.election.ElectionState
import `in`.xnnyygn.xraft2.election.Role

class LogReplicatorCell(private val connections: CellRef) : Cell() {
    /**
     * copy of election state of current node
     */
    private var electionState = ElectionState(Role.FOLLOWER, 1)

    override fun receive(context: CellContext, event: CellEvent) {
        TODO("Not yet implemented")
    }
}