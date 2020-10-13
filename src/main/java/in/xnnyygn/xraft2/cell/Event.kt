package `in`.xnnyygn.xraft2.cell

interface Event

object EmptyEvent: Event

object PoisonPill : Event

open class CellEvent(val sender: CellRef) : Event {
    fun reply(event: Event) {
        sender.tell(event)
    }
}