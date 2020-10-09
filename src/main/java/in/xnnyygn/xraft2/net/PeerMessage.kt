package `in`.xnnyygn.xraft2.net

interface PeerMessage

data class HandshakeRpc(val name: String): PeerMessage
data class HandshakeReply(val name: String): PeerMessage

class RequestVoteRpc: PeerMessage {
}