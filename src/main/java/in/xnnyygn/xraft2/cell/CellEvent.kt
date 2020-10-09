package `in`.xnnyygn.xraft2.cell

interface CellEvent

object PoisonPill : CellEvent

abstract class OwnedEvent(val sender: CellRef) : CellEvent {
    open fun reply(event: CellEvent) {
        sender.tell(event)
    }
}