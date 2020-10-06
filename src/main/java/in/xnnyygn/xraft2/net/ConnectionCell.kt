package `in`.xnnyygn.xraft2.net

import `in`.xnnyygn.xraft2.cell.Cell
import `in`.xnnyygn.xraft2.cell.CellContext
import `in`.xnnyygn.xraft2.cell.Message

class ConnectionCell: Cell() {
    override fun receive(context: CellContext, msg: Message) {
        TODO("Not yet implemented")
    }
}

class PeerRpcMessage(val rpc: PeerRpc): Message