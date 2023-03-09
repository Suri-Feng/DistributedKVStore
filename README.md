WIP
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
