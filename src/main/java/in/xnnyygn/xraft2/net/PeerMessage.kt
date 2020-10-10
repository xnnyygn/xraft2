package `in`.xnnyygn.xraft2.net

interface PeerMessage

data class HandshakeRpc(val name: String)
data class HandshakeReply(val name: String)

class RequestVoteRpc: PeerMessage {
}

class RequestVoteReply: PeerMessage {

}

class AppendEntriesRpc: PeerMessage {

}

class AppendEntriesReply: PeerMessage {

}