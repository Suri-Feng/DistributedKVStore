#!/bin/bash
meta='info.txt'
client_private_ip=$(echo `grep -i "Client-private-ip" $meta` | cut -d ":" -f 2) 
nodefile="nodes-list.txt"
 > $nodefile
echo "$client_private_ip:43100" >> $nodefile
echo "generating nodes-list.txt file on client $client_private_ip for 43100 port" 