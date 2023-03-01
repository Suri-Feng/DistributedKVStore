#!/bin/bash
nodefile='nodes-list.txt'
jarfile='A7.jar'
while read line || [ -n "$line" ]; do 
port=$(echo "$line" | cut -d ":" -f 2)
echo "Starting server on port $port"
java -Xmx64m -jar $jarfile $port &> /dev/null &
done < $nodefile