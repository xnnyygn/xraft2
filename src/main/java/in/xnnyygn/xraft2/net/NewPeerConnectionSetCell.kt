package `in`.xnnyygn.xraft2.net

import `in`.xnnyygn.xraft2.NameDuplicatedEvent
import `in`.xnnyygn.xraft2.cell.*
import `in`.xnnyygn.xraft2.log.NewPeerLogReplicationDoneEvent
import `in`.xnnyygn.xraft2.log.NewPeerLogReplicatorCell
import io.netty.channel.Channel
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.util.concurrent.Future

typealias CloseListener = (future: Future<in Void>) -> Unit

class NewPeerConnectionSetCell(
    private val nodeName: String,
    private val workerGroup: NioEventLoopGroup
) : Cell() {
    private val taskMap = mutableMapOf<String, NewPeerCellEvent>()
    private val connectionMap = mutableMapOf<NodeAddress, NewPeerConnection>()

    override fun receive(context: CellContext, event: Event) {
        when (event) {
            is NewPeerCellEvent -> addNewPeer(context, event)
            is IncomingHandshakeHandler.IncomingChannelEvent -> incomingChannel(context, event)
            is OutgoingChannelEvent -> outgoingChannel(context, event)
            is NewPeerLogReplicationDoneEvent -> replicationDone(context, event)
            is UpgradeNewPeerEvent -> upgradeNewPeer(context, event)
            is ConnectionClosedEvent -> connectionClosed(context, event)
        }
    }

    private fun connectionClosed(context: CellContext, event: ConnectionClosedEvent) {
        val address = event.address
        val task = taskMap.remove(address.name)
        if (task == null) {
            context.logger.warn("unknown new peer ${address.name}")
        } else {
            context.logger.info { "connection closed, node ${address.name}" }
            task.reply(event)
            connectionMap.remove(address)?.stopReplicator()
        }
    }

    private fun outgoingChannel(context: CellContext, event: OutgoingChannelEvent) {
        val address = event.address
        val channel = event.channel
        if (!taskMap.containsKey(address.name)) {
            context.logger.warn("new peer has been removed, node ${address.name}")
            channel.close()
        } else if (connectionMap.containsKey(address)) {
            context.logger.warn("duplicated connection of node ${address.name}")
            channel.close()
        } else {
            addNewPeerConnection(context, channel, address)
        }
    }

    private fun incomingChannel(context: CellContext, event: IncomingHandshakeHandler.IncomingChannelEvent) {
        val channel = event.channel
        val task = taskMap[event.remoteName]
        if (task == null) {
            context.logger.warn("unknown node ${event.remoteName}")
            channel.close()
            return
        }
        val address = task.address
        if (connectionMap.containsKey(address)) {
            context.logger.warn("duplicated connection of node ${address.name}")
            channel.close()
        } else {
            event.reply()
            addNewPeerConnection(context, channel, address)
        }
    }

    private fun addNewPeer(context: CellContext, event: NewPeerCellEvent) {
        val address = event.address
        if (taskMap.containsKey(address.name)) {
            event.reply(NameDuplicatedEvent(address))
        } else {
            taskMap[event.address.name] = event
            context.startChild(ClientCell(nodeName, event.address, workerGroup))
        }
    }

    private fun replicationDone(context: CellContext, event: NewPeerLogReplicationDoneEvent) {
        val address = event.address
        val task = taskMap[address.name]
        if (task == null) {
            context.logger.warn { "unknown new peer ${address.name}" }
        } else {
            task.reply(event)
        }
    }

    private fun upgradeNewPeer(context: CellContext, event: UpgradeNewPeerEvent) {
        val address = event.address
        val task = taskMap.remove(address.name)
        if (task == null) {
            context.logger.warn { "unknown new peer ${address.name}" }
            return
        }
        val connection = connectionMap.remove(address)
        if (connection == null) {
            context.logger.warn { "no such connection to node ${address.name}" }
            return
        }
        connection.removeCloseListener()
        context.parent.tell(NewPeerChannelEvent(address, connection.channel))
    }

    private fun addNewPeerConnection(context: CellContext, channel: Channel, address: NodeAddress) {
        val closeListener: CloseListener = {
            context.self.tell(ConnectionClosedEvent(address))
        }
        channel.closeFuture().addListener(closeListener)
        val replicator = context.startChild(NewPeerLogReplicatorCell(address, channel))
        connectionMap[address] = NewPeerConnection(channel, closeListener, replicator)
    }
}

internal class NewPeerConnection(
    val channel: Channel,
    private val closeListener: CloseListener,
    private val replicator: CellRef
) {
    fun removeCloseListener() {
        channel.closeFuture().removeListener(closeListener)
    }

    fun stopReplicator() {
        replicator.tell(PoisonPill)
    }
}

class UpgradeNewPeerEvent(val address: NodeAddress) : Event
class NewPeerChannelEvent(val address: NodeAddress, val channel: Channel) : Event