#!/bin/bash
META='info.txt'
if [ -f "$META" ]; then
    export server_public_ip=$(echo `grep -i "Server-public-ip" $META` | cut -d ":" -f 2)
    export number_of_nodes=$(echo `grep -i "Number-of-nodes" $META` | cut -d ":" -f 2)
    export first_port=$(echo `grep -i "First-port" $META` | cut -d ":" -f 2)
else
    echo "$META does not exist. Abort!"
    exit
fi
serverlistfile="servers.txt"

> $serverlistfile
last_port=$(expr $first_port + $number_of_nodes)
last_port=$(expr $last_port - 1)
for port in `seq $first_port $last_port`
do 
    echo "$server_public_ip:$port" >> $serverlistfile
done 
echo "generating servers.txt on client for servers $server_public_ip from port $first_port to $last_port"

