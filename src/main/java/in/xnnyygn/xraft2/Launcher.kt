package `in`.xnnyygn.xraft2

import `in`.xnnyygn.xraft2.cell.CellSystem

fun main(args: Array<String>): Unit {
    val system = CellSystem()
    system.add(InitializerCell())
    system.start()
    System.`in`.read()
    system.stop()
}