#!/bin/bash
nodefile='nodes-list.txt'
jarfile='A7.jar'
while read line || [ -n "$line" ]; do 
port=$(echo "$line" | cut -d ":" -f 2) 
pid=$(lsof -i :$port -t)
[[ ! -z "$pid" ]] && kill -9 $pid && echo "Shut down server process with pid $pid on port $port"
done < $nodefile