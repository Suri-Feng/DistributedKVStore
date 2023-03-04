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
  Below are 8 usage
  ```bash
  # transfer all related files
  bash aws_comm.sh transport_to # no second arg, tranport to both
  bash aws_comm.sh transport_to server
  bash aws_comm.sh transport_to client
  # ssh
  bash aws_comm.sh ssh server
  bash aws_comm.sh ssh client
  # transfer back log file
  bash aws_comm.sh transport_back server
  bash aws_comm.sh transport_back one-server
  bash aws_comm.sh transport back client 
  ```
  Remark: This should be run locally.

- setting env respectively on aws client machine/ aws server machine
  ```bash
  bash aws_env.sh <setup/set_netem/ del_netem>
  # only set_netem have further args
  ```
  - This should be run on aws machines
  - ***iface*** are lo and ens5
  - ***bash aws_env.sh set_netem delay 5*** add 5msec delay
  - ***bash aws_env.sh set_netem loss 2.5*** add 2.5% loss


### Run Server/ Client
- change params in info.txt
  - server private ip, server public ip, one server private ip
  - **number of nodes of choice**, port of choice
  - server jar file, client jar file
  - submission secret code
- start server and client 
  ```bash
  bash run.sh <server/one-server/client> # java -Xmx64m -jar $jarfile $port
  ```
  - ***bash run.sh server***
    - This is based on the assumption that the client and the servers are deployed on two different machine
    - ***bash run.sh server*** will create a ***nodes-list.txt*** in format of <server_private_ip, port> with ***n ports***, if there doesn't exist one
    - server nodes will be run in background, and outputs saved in ***nodes_output.log***
  - ***bash run.sh one-server***
    - This is based on the assumption that the client and the server are deployed on the same machine
    - ***bash run.sh one-server*** will create a ***nodes-list.txt*** in format of <client_private_ip, 43100> with ***1 port***, if there doesn't exist one
    - one server node will be run in background, and outputs saved in ***nodes_output.log***
  - ***bash run.sh client***
    - client will be run in foreground
    - with optional argument, ***bash run.sh client submit*** will create a ***servers.txt*** in format of <server_public_ip, port> with n ports, if there doesn't exist one; 
    and submit to the leaderboard
  - Remarks
    - First argument ***<server/one-server/client>*** is compulsory
    - ***nodes-list.txt*** and ***servers.txt*** will not be overwritten if there exists one
- kill all alive nodes accoring to ***nodes-list.txt***
  ```
  bash kill_nodes.sh
  ```
