## CPEN 431 2022W2 Assignment 9

Group ID: g3

Verification code: 7A9F932D0DE506BC62B29EF5E5B0550F

### Usage
To run the compiled jar file located in the root directory, run the following command:

`java -Xmx512m -jar A9.jar <port number>`

The server will start on the specified port number.

### Brief Description
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