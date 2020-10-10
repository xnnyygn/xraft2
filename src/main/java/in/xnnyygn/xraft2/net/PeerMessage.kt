package `in`.xnnyygn.xraft2.net

interface PeerMessage

data class HandshakeRpc(val name: String)
data class HandshakeReply(val name: String)

class RequestVoteRpc(
    val term: Int,
    val candidateId: String,
    val lastLogIndex: Int,
    val lastLogTerm: Int
): PeerMessage {
}

class RequestVoteReply(val term: Int, val voteGranted: Boolean): PeerMessage

class AppendEntriesRpc(
    val term: Int,
    val leaderId: String,
    val prevLogIndex: Int,
    val prevLogTerm: Int,
    val entries: List<Int>,
    val leaderCommit: Int
): PeerMessage

class AppendEntriesReply(val term: Int, val success: Boolean): PeerMessage

class InstallSnapshotRpc(
    val lastIndex: Int
): PeerMessage {

}

class InstallSnapshotReply(val term: Int, val success: Boolean): PeerMessage