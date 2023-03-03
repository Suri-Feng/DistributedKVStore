#!/bin/bash
META='info.txt'
if [ -f "$META" ]; then
    export server_private_ip=$(echo `grep -i "Server-private-ip" $META` | cut -d ":" -f 2)
    export number_of_nodes=$(echo `grep -i "Number-of-nodes" $META` | cut -d ":" -f 2)
    export first_port=$(echo `grep -i "First-port" $META` | cut -d ":" -f 2)
else
    echo "$META does not exist. Abort!"
    exit
fi

if [ -z "$number_of_nodes" ] || [ -z "$first_port" ]
then
  echo "No port number and/ or number of nodes are set. Abort!"
  exit
fi

nodefile="nodes-list.txt"
> $nodefile
last_port=$(expr $first_port + $number_of_nodes)
last_port=$(expr $last_port - 1)
for port in `seq $first_port $last_port`
do 
    echo "$server_private_ip:$port" >> $nodefile
done 
echo "generating nodes-list.txt for server nodes $server_private_ip from port $first_port to $last_port"