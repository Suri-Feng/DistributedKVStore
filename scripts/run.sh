#!/bin/bash

META='info.txt'

if [[ ! -f "$META" ]]; then
  echo "$META does not exist. Abort!"
  exit
fi

if [[ $1 == client ]]; then
  export jarfile=$(echo `grep -i "Client_jar_file" $META` | cut -d ":" -f 2)
else
  export jarfile=$(echo `grep -i "Server_jar_file" $META` | cut -d ":" -f 2)
fi

generate-node-file()
{
    nodefile='nodes-list.txt'
    if [[ ! -f "$nodefile" ]]; then
      echo "node-list.txt doesn't exist. generating new ..."
      bash aws_nodefile.sh "$1"
    fi
}

generate-server-list-file()
{
    serverlistfile="servers.txt"

    if [[ ! -f "$serverlistfile" ]]; then
      echo "servers.txt doesn't exist. generating new ..."
      bash aws_serverlistfile_client.sh
    fi
}

if [[ $1 == server ]] || [[ $1 == one-server ]] ; then

    generate-node-file $1

    > nodes_output.log

    while read line || [ -n "$line" ]; do
      port=$(echo "$line" | cut -d ":" -f 2)
      echo "Starting server on port $port using $jarfile"
      java -Xmx64m -jar $jarfile $port >> nodes_output.log 2>&1 &
    done < $nodefile

elif [[ $1 == client ]]; then

    generate-server-list-file

    if [[ $2 == submit ]]; then
        submit_code=$(echo `grep -i "Submit-secret-code" $META` | cut -d ":" -f 2)
        echo "client is running in submission mode."
        java -jar $jarfile -servers-list=servers.txt -submit -secret-code $submit_code
    else
        echo "client is running in test mode."
        java -jar $jarfile -servers-list=servers.txt
    fi

fi
