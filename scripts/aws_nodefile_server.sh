#!/bin/bash
meta='info.txt'
server_private_ip=$(echo `grep -i "Server-private-ip" $meta` | cut -d ":" -f 2) 
number_of_nodes=$(echo `grep -i "Number-of-nodes" $meta` | cut -d ":" -f 2) 
first_port=$(echo `grep -i "First-port" $meta` | cut -d ":" -f 2) 
nodefile="nodes-list.txt"
rm $nodefile
touch $nodefile
last_port=$(expr $first_port + $number_of_nodes)
last_port=$(expr $last_port - 1)
for port in `seq $first_port $last_port`
do 
    echo "$server_private_ip:$port" >> $nodefile
done 
echo "generating nodes-list.txt on server $server_private_ip from port $first_port to $last_port"