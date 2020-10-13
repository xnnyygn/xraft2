package `in`.xnnyygn.xraft2

import `in`.xnnyygn.xraft2.cell.*

class NodeListCell(private val raftLog: CellRef, private val connectionSet: CellRef, private val election: CellRef) : Cell() {
    override fun start(context: CellContext) {
        val event = RegisterNodeListEvent(context.self)
        raftLog.tell(event)
        election.tell(event)
    }

    override fun receive(context: CellContext, event: Event) {
        TODO("Not yet implemented")
        // initial node list
        // add new peer, ask Election to check role
        // remove new peer, ask Election to check role
        // group config committed -> new peer? upgrade, normal, update connection set, update election
    }
}

class RegisterNodeListEvent(val nodeList: CellRef): Event
class DisableNodeListUpdateCellEvent(sender: CellRef): CellEvent(sender) {
    fun reply() {
        reply(NodeListUpdateDisabledEvent)
    }
}
object NodeListUpdateDisabledEvent: Event