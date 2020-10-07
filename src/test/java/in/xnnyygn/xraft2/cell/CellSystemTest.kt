package `in`.xnnyygn.xraft2.cell

import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import java.util.concurrent.TimeUnit

class CellSystemTest {
    inner class SimpleCell : Cell() {
        override fun start(context: CellContext) {
            context.schedule(500L, TimeUnit.MILLISECONDS, PrintMessage)
        }

        override fun receive(context: CellContext, msg: Message) {
            if (msg == PrintMessage) {
                println("hello")
                context.stopSelf()
            }
        }
    }

    object PrintMessage : Message

    @Test
    @Disabled
    fun test() {
        val system = CellSystem()
        system.add(SimpleCell())
        system.start()
        Thread.sleep(1000L)
        system.stop()
    }
}