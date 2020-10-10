package `in`.xnnyygn.xraft2.election

import `in`.xnnyygn.xraft2.NodeState

open class ElectionState(val role: Role, val term: Int)

class FollowerState(term: Int, var votedFor: String?) : ElectionState(Role.FOLLOWER, term) {
    constructor(nodeState: NodeState) : this(nodeState.term, nodeState.votedFor)
}

class CandidateState(term: Int) : ElectionState(Role.CANDIDATE, term) {
    private var _voteCount: Int = 1

    fun increaseVoteCount(): Int {
        _voteCount++
        return _voteCount
    }
}

class LeaderState(term: Int) : ElectionState(Role.LEADER, term)