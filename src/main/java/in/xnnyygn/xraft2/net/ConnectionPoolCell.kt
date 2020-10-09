package `in`.xnnyygn.xraft2.net

import `in`.xnnyygn.xraft2.cell.Cell
import `in`.xnnyygn.xraft2.cell.CellContext
import `in`.xnnyygn.xraft2.cell.CellRef
import `in`.xnnyygn.xraft2.cell.CellEvent
import io.netty.channel.Channel
import io.netty.channel.nio.NioEventLoopGroup

class ConnectionPoolCell(
    private val nodeName: String,
    private val addresses: MutableList<NodeAddress>,
    private val workerGroup: NioEventLoopGroup
) : Cell() {
    private val addressMap = addresses.associateBy { it.name }
    private val connectionMap = mutableMapOf<NodeAddress, CellRef>()
    private val pendingMessagesMap = mutableMapOf<NodeAddress, MutableList<PeerMessage>>()

    override fun receive(context: CellContext, event: CellEvent) {
        when (event) {
            is IncomingHandshakeHandler.IncomingChannelEvent -> incomingChannel(context, event)
            is OutgoingChannelEvent -> outgoingChannel(context, event)
            is PeerMessageEvent -> broadcast(context, event)
            is ClientConnectionFailedEvent -> pendingMessagesMap.remove(event.address)
            is ConnectionClosedEvent -> connectionMap.remove(event.address)
        }
    }

    private fun incomingChannel(context: CellContext, event: IncomingHandshakeHandler.IncomingChannelEvent) {
        val address = addressMap[event.remoteName]
        when {
            address == null -> {
                context.logger.warn("unknown node ${event.remoteName}")
                event.channel.close()
            }
            connectionMap.containsKey(address) -> {
                context.logger.warn("duplicated connection of node ${address.name}")
                event.channel.close()
            }
            else -> {
                event.reply()
                addConnection(context, event.channel, address)
            }
        }
    }

    private fun outgoingChannel(context: CellContext, event: OutgoingChannelEvent) {
        if (connectionMap.containsKey(event.address)) {
            context.logger.warn("duplicated connection of node ${event.address.name}")
            event.channel.close()
        } else {
            addConnection(context, event.channel, event.address)
        }
    }

    private fun broadcast(context: CellContext, event: PeerMessageEvent) {
        for (address in addresses) {
            val connectionRef = connectionMap[address]
            if (connectionRef != null) { // connected
                connectionRef.tell(event)
                continue
            }
            val queue = pendingMessagesMap[address]
            if (queue != null) { // connecting
                // still connecting
                queue.add(event.message)
                continue
            }
            // new connection
            context.startChild(ClientCell(nodeName, address, workerGroup))
            pendingMessagesMap[address] = mutableListOf(event.message)
        }
    }

    private fun addConnection(context: CellContext, channel: Channel, address: NodeAddress) {
        val connectionRef = context.startChild(ConnectionCell(address, channel))
        connectionMap[address] = connectionRef
        val queue = pendingMessagesMap.remove(address)
        if (queue != null) {
            connectionRef.tell(PendingMessagesEvent(queue))
        }
    }

    override fun stop(context: CellContext) {
        pendingMessagesMap.clear()
    }
}

