package `in`.xnnyygn.xraft2.net

import java.net.InetSocketAddress

data class NodeAddress(val name: String, val ip: String, val port: Int) {
    constructor(ip: String, port: Int) : this(ip, ip, port)
    constructor(address: InetSocketAddress) : this(address.hostName, address.hostString, address.port)
}