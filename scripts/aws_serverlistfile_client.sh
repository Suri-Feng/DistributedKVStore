#!/bin/bash
meta='info.txt'
server_public_ip=$(echo `grep -i "Server-public-ip" $meta` | cut -d ":" -f 2) 
number_of_nodes=$(echo `grep -i "Number-of-nodes" $meta` | cut -d ":" -f 2) 
first_port=$(echo `grep -i "First-port" $meta` | cut -d ":" -f 2) 
serverlistfile="servers.txt"
rm $serverlistfile
touch $serverlistfile
last_port=$(expr $first_port + $number_of_nodes)
last_port=$(expr $last_port - 1)
for port in `seq $first_port $last_port`
do 
    echo "$server_public_ip:$port" >> $serverlistfile
done 
echo "generating servers.txt on client for servers $server_public_ip from port $first_port to $last_port"