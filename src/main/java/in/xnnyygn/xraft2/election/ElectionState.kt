package `in`.xnnyygn.xraft2.election

import `in`.xnnyygn.xraft2.NodeState

open class ElectionState(val role: Role, val term: Int)

class FollowerState(term: Int, val votedFor: String?) : ElectionState(Role.FOLLOWER, term) {
    constructor(nodeState: NodeState) : this(nodeState.term, nodeState.votedFor)
}

class CandidateState(term: Int) : ElectionState(Role.CANDIDATE, term)