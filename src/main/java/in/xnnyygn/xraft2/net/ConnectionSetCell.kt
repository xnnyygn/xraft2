package `in`.xnnyygn.xraft2.net

import `in`.xnnyygn.xraft2.cell.*
import io.netty.channel.Channel
import io.netty.channel.nio.NioEventLoopGroup

class ConnectionSetCell(
    private val nodeName: String,
    addresses: List<NodeAddress>,
    private val workerGroup: NioEventLoopGroup
) : Cell() {
    private var _newPeerConnectionSet: CellRef? = null
    private val addressMap = addresses.associateByTo(mutableMapOf()) { it.name }
    private val connectionMap = mutableMapOf<NodeAddress, CellRef>()
    private val pendingMessagesMap = mutableMapOf<NodeAddress, PendingMessageQueue>()
    private var logReplicationEnabled = false

    private val newPeerConnectionSet: CellRef
        get() = _newPeerConnectionSet!!

    override fun start(context: CellContext) {
        _newPeerConnectionSet = context.startChild(NewPeerConnectionSetCell(nodeName, workerGroup))
    }

    override fun receive(context: CellContext, event: Event) {
        when (event) {
            is IncomingHandshakeHandler.IncomingChannelEvent -> incomingChannel(context, event)
            is OutgoingChannelEvent -> outgoingChannel(context, event)
            is PeerMessageEvent -> broadcast(context, event)
            is EnableLogReplicationEvent -> enableOrDisableLogReplication(context, event, true)
            is DisableLogReplicationEvent -> enableOrDisableLogReplication(context, event, false)
            is NewPeerCellEvent -> addNewPeer(event)
            is NewPeerChannelEvent -> upgradeNewPeer(context, event)
            is ClientConnectionFailedEvent -> pendingMessagesMap.remove(event.address)
            is ConnectionClosedEvent -> connectionMap.remove(event.address) // TODO also remove from newPeerLogReplicatorMap
        }
    }

    private fun upgradeNewPeer(context: CellContext, event: NewPeerChannelEvent) {
        val address = event.address
        addressMap[address.name] = address
        addConnection(context, event.channel, address)
    }

    private fun addNewPeer(event: NewPeerCellEvent) {
        val address = event.address
        if (address.name == nodeName || addressMap.containsKey(address.name)) {
            event.reply(NameDuplicatedEvent(address))
            return
        }
        newPeerConnectionSet.tell(event) // forward to NewPeerConnectionSet
    }


    private fun incomingChannel(context: CellContext, event: IncomingHandshakeHandler.IncomingChannelEvent) {
        val channel = event.channel
        val address = addressMap[event.remoteName]
        when {
            address == null -> {
                // might be new peer, forward to NewPeerConnectionSet
                newPeerConnectionSet.tell(event)
            }
            connectionMap.containsKey(address) -> {
                context.logger.warn("duplicated connection of node ${address.name}")
                channel.close()
            }
            else -> {
                event.reply()
                addConnection(context, channel, address)
            }
        }
    }

    private fun outgoingChannel(context: CellContext, event: OutgoingChannelEvent) {
        val channel = event.channel
        val address = event.address
        if (!addressMap.containsKey(address.name)) {
            context.logger.warn("node ${address.name} has been removed")
            channel.close()
        } else if (connectionMap.containsKey(address)) {
            context.logger.warn("duplicated connection of node ${address.name}")
            channel.close()
        } else {
            addConnection(context, channel, address)
        }
    }

    private fun enableOrDisableLogReplication(context: CellContext, event: Event, enabled: Boolean) {
        context.logger.info("logReplicationEnabled -> $enabled")
        logReplicationEnabled = enabled
        for (connection in connectionMap.values) {
            connection.tell(event)
        }
        // new connection will be notified when be added via PendingMessageEvent
    }

    private fun broadcast(context: CellContext, event: PeerMessageEvent) {
        for (address in addressMap.values) {
            val connectionRef = connectionMap[address]
            if (connectionRef != null) { // connected
                connectionRef.tell(event)
                continue
            }
            val queue = pendingMessagesMap[address]
            if (queue != null) { // connecting
                // still connecting
                queue.offer(event.message)
                continue
            }
            // new connection
            context.startChild(ClientCell(nodeName, address, workerGroup))
            pendingMessagesMap[address] = PendingMessageQueue(event.message)
        }
    }

    private fun addConnection(context: CellContext, channel: Channel, address: NodeAddress) {
        channel.closeFuture().addListener {
            context.logger.info { "connection closed, node $address" }
            context.self.tell(ConnectionClosedEvent(address))
        }
        val connection = context.startChild(ConnectionCell(address, channel))
        connectionMap[address] = connection
        val queue = pendingMessagesMap.remove(address)
        connection.tell(PendingMessageEvent(queue, logReplicationEnabled))
    }

    override fun stop(context: CellContext) {
        pendingMessagesMap.clear()
    }
}

internal class PendingMessageQueue(firstMessage: PeerMessage) {
    private var _lastMessage: PeerMessage = firstMessage

    val lastMessage: PeerMessage
        get() = _lastMessage

    fun offer(message: PeerMessage) {
        _lastMessage = message
    }
}

class NewPeerCellEvent(val address: NodeAddress, sender: CellRef) : CellEvent(sender)
class NameDuplicatedEvent(val address: NodeAddress) : Event

internal class PendingMessageEvent(val queue: PendingMessageQueue?, val logReplicationEnabled: Boolean) : Event