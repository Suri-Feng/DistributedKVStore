#!/bin/bash

META='info.txt'
if [ -f "$META" ]; then
  export jarfile=$(echo `grep -i "Server_jar_file" $META` | cut -d ":" -f 2)
  echo "setting server jarfile variable from info.txt"
else
  echo "$META does not exist. Abort!"
  exit
fi

nodefile='nodes-list.txt'
# shellcheck disable=SC1035
if [[ ! -f "$nodefile" ]]; then
  echo "node-list.txt doesn't exist. generating new ..."
  bash aws_nodefile_server.sh
fi

serverlistfile="servers.txt"
if [[ ! -f "$serverlistfile" ]] && [[ $1 == servers ]]; then
    echo "You want to create a servers.txt together."
    bash aws_serverlistfile_client.sh
fi

> nodes_output.log

while read line || [ -n "$line" ]; do
  port=$(echo "$line" | cut -d ":" -f 2)
  echo "Starting server on port $port using $jarfile"
  java -Xmx64m -jar $jarfile $port >> nodes_output.log 2>&1 &
done < $nodefile