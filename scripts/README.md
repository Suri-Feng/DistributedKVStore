### AWS
- edit params in info.txt
- move following file to the same folder
  - client jar 
  - server jar 
  - cpen431_pop.pub 
  - aws pem to the same folder
- communicate with aws - scp/ ssh
  ```bash
  bash aws_comm.sh <ssh/trasnport_to/transport_back> <client/server>
  ```
  - If no client/ server arg provided for transport_to case, will transport to both
- setting env respectively on aws client machine/ aws server machine
  ```bash
  bash aws_env_client.sh 
  bash aws_env_server.sh
  ```
- in env.sh - simulating network latency
  ```bash
  #TODO
  sudo tc qdisc add dev lo   root netem delay 5msec loss 2.5%
  sudo tc qdisc add dev ens5 root netem delay 5msec loss 2.5%
  sudo tc qdisc del dev <iface> root
  ```

### Run Server/ Client
- change params in info.txt
  - server private ip [1], server public ip [2]
  - **number of nodes of choice**, port of choice
  - server jar file, client jar file [3]
  - submit secret code [4]
  - [1] if you want to have a proper private ip in nodes-list.txt
  - [2] if you need to create servers.txt for the test client
  - [3] if you want to run client
  - [4] if you want to run client in submit mode
- start server and client 
  ```bash
  bash nodes_run.sh <servers> # java -Xmx64m -jar $jarfile $port
  bash client_run.sh <submit> # java -Xmx64m -jar $jarfile $port
  ```
  - ***bash nodes_run.sh*** will create a ***nodes-list.txt*** <private_ip, port>, if there doesn't exist one
  - ***bash nodes_run.sh servers*** will create a ***servers.txt*** <public_ip, port>, if there doesn't exist one
  - The two txt file can be created manually using ***aws_nodefile_server.sh***, and ***aws_serverlistfile_client.sh***
  - Server output saved in ***nodes_output.log***
- kill all alive nodes
  ```
  bash nodes_kill.sh 
  ```
