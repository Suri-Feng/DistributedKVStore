## CPEN 431 2022W2 Assignment 12

Group ID: G3

Verification code: 10B70185CB311EE931119E954AAE2A15

### Usage
To run the compiled jar file located in the root directory, run the following command:

`java -Xmx512m -jar 12.jar <port number>`

The server will start on the specified port number.

### Brief Descriptions
#### A12
- We have switched to using a blocking primary/backup replication protocol for PUT requests, wherein primary nodes wait for 3 replicas' 
  ACKs before responding to clients. PUT requests are inserted into the primary node's queue and will be removed 
  either after 3 ACKs have been received at which point a SUCCESS reply will be sent to the client or after 4 seconds of insertion, whichever occurs first.
- To maintain the order of events across nodes, a write_timestamp is appended to each key when it is first entered into the system. 
   For subsequent PUT requests on the same key, we verify whether the operation time is earlier than the write_timestamp in the system. 
   If it is, we reject the operation; otherwise, we perform the PUT and update the write_timestamp to the new operation time. During key transfers, 
   these write_timestamps are synchronized across the replicas to ensure that nodes see the same sequence of operations.
#### A11
- A replication factor of 4 is used: one primary and three backups. The first node that a key is mapped to on the consistent 
  hashing ring is the primary node for the key, and the following three nodes are the replicas.
- To achieve load balancing, all the nodes mentioned are virtual nodes. Each physical node has three virtual nodes on the ring. 
  As different keys can be mapped to different virtual nodes associated with the same physical node, it is possible for keys that are mapped to 
  the same primary node to have different replicas.
- To minimize latency, a non-blocking primary-backup protocol is implemented. The primary node is responsible for processing and responding to 
  clients' PUT/GET/REMOVE requests, and requests are forwarded to replicas asynchronously.
- When nodes are suspended or resumed, keys can be mapped to new primaries depending on whether they will be mapped to new nodes on the updated ring. 
  Key transfers are followed by remapping. For instance, when a key's primary node fails, the first backup is elected to be the new primary and 
  transfer the key-value pair to the new backup(s). Similarly, when a key's backup nodes are updated, the primary transfers keys to new backups. 
  When a key's primary is recovered, the first backup transfers keys to the resumed primary. Continuous transferring is applied here to ensure that 
  the real primary receives the key-value pairs.
- The epidemic protocol is implemented lazily. A node updates its consistent hashing ring (adding/deleting nodes on the circle) only after it 
  receives a heartbeat (gossip) message from other nodes. When a node receives a heartbeat list from other nodes, it always uses it to update its 
  own heartbeat list. However, it only uses this information to update its ring if its own entry on the heartbeat list is recent enough. 
  This ensures that the messages a node uses to update its ring are not stale and avoids mistakenly removing active nodes from the ring with stale
  messages. This approach works well for nodes that are suspended and resumed.
#### A9
- To recover nodes that have been resumed from a temporary failure, alive nodes upon receiving new heartbeats check if
  any previously dead nodes have become alive and add them back to the circle. To remap the keys, nodes that have
  discovered the resumed nodes will send a message to the resumed nodes' successors with the affected key hash ranges. The 
  successors will then look for keys within these ranges and transfer them to the recovered node.
#### A7
- Consistent hashing is used to determine the interested node for PUT/GET/REMOVE commands. 
  If another node other than self should handle the request, the request is first appended with the client
  address and port before forwarding to the correct node. The correct node will then process the request and send the
  response back to the client.
- The push epidemic protocol is used to detect node failures. Each node keeps a list of heartbeats of all nodes, and at 
  a set interval (10ms), sends its own list to 2 other randomly selected nodes. When a node receives a list, it updates
  its own list by taking the larger value of each entry. If the heartbeat for any node at any time hasn't been updated
  for some set period, the node will be treated as dead. To prevent false positives due to network latency (i.e. regarding a
  node as dead when it's actually not), a recovery mechanism is used such that if a dead node later is found alive again,
  it will be added back to the node list.