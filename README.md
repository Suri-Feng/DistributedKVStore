## CPEN 431 2022W2 Assignment 11

Group ID: g3

Verification code: A7CEFCA54F86F49F870EB540FF12724B

### Usage
To run the compiled jar file located in the root directory, run the following command:

`java -Xmx512m -jar 1.jar <port number>`

The server will start on the specified port number.

### Brief Descriptions
#### A11
- One key would have 4 replica: one primary and three backups. The node that a key can be mapped on the consistent 
  hashing ring would be the primary node for a key, and the following three nodes would be the backups for the key.
  To achieve load balancing, all the nodes mentioned are virtual nodes. One physical node has three virtual nodes on the ring.
  Different keys that mapped to the same primary node might have different backups, as they can be mapped to different 
  virtual nodes the physical node associated with.
- Non-blocking primary-backup protocol is implemented to achieve lower latency: only the primary node can be written and read. 
  Whenever a client sends PUT/REM to the primary node a key is associated with, the primary node will send instructions to the 
  backups for PUT/REM, and continues to execute without waiting for any ACK. 
- When nodes are suspended or resumed, a key can be mapped to new primary depending on whether it will be mapped to a new node on the updated ring. Key transfers will 
  be followed by those remapping: when a key's primary node dies, the first backup is elected to be the new primary, and transfer
  the key-value pair to the new backup(s); when a key's backup nodes updated, the primary will transfer keys to new backups; 
  when a key's primary is recovered, the first backup will transfer keys to the resumed primary, continuous transferring is applied 
  here to ensure real primary will receive the key-value pairs. 
- The epidemic protocol is lazily implemented. The only condition that a node is going to update its consistent hashing ring 
  (adding/ deleting nodes on the circle) after it receives a heartbeat (gossip) message from other nodes. When receiving a heartbeats  
  list from other nodes, a node will always use this to update its own heartbeat list; however, it won't use this information 
  to update its ring unless its own entry on the heartbeat list is quite recent - this can guarantee that the messages a node uses 
  to update its ring will not be stale, and avoid mistakenly removing active nodes from the ring with stale messages, which works
  good for nodes were suspended and resumed.
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