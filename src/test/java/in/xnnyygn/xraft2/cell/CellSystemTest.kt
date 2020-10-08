package `in`.xnnyygn.xraft2.cell

import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

class CellSystemTest {
    class SchedulerCell(private val countDownLatch: CountDownLatch) : Cell() {
        override fun start(context: CellContext) {
            context.schedule(100L, TimeUnit.MILLISECONDS, PrintMessage)
        }

        override fun receive(context: CellContext, msg: Message) {
            if (msg == PrintMessage) {
                context.logger.info("hello")
                countDownLatch.countDown()
            }
        }
    }

    object PrintMessage : Message {
        override fun toString() = "PrintMessage"
    }

    @Test
    @Disabled("time consuming")
    fun testScheduler() {
        val countDownLatch = CountDownLatch(1)
        val system = CellSystem()
        system.add(SchedulerCell(countDownLatch))
        system.start()
        countDownLatch.await()
        system.stop()
    }

    class ParentCell(private val countDownLatch: CountDownLatch) : Cell() {
        override fun start(context: CellContext) {
            context.startChild(ChildCell())
        }

        override fun receive(context: CellContext, msg: Message) {
            if (msg == ChildStartedMessage) {
                context.logger.info("child started")
                countDownLatch.countDown()
            }
        }

        override fun stop(context: CellContext) {
            context.logger.info("stop parent")
        }
    }

    class ChildCell : Cell() {
        override fun start(context: CellContext) {
            context.parent.send(ChildStartedMessage)
        }

        override fun receive(context: CellContext, msg: Message) {
        }

        override fun stop(context: CellContext) {
            context.logger.info("stop child")
        }
    }

    object ChildStartedMessage : Message {
        override fun toString() = "ChildStartedMessage"
    }

    @Test
    fun testParentChild() {
        val countDownLatch = CountDownLatch(1)
        val system = CellSystem()
        system.add(ParentCell(countDownLatch))
        system.start()
        countDownLatch.await()
        system.stop()
    }
}