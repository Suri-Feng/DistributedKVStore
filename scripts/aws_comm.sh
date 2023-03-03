export meta='info.txt'
export key=$(echo `grep -i "Key" $meta` | cut -d ":" -f 2) 
export server_public_ip=$(echo `grep -i "Server-public-ip" $meta` | cut -d ":" -f 2) 
export client_public_ip=$(echo `grep -i "Client-public-ip" $meta` | cut -d ":" -f 2) 

# transferring 
#jar 
export server_jar_file=$(echo `grep -i "Server_jar_file" $meta` | cut -d ":" -f 2) 
export client_jar_file=$(echo `grep -i "Client_jar_file" $meta` | cut -d ":" -f 2) 
# meta
export info_txt='info.txt'
export public_key='cpen431_pop.pub'
# static params 
# env setting sh
export client_env_sh='aws_env_client.sh'
export server_env_sh='aws_env_server.sh'
# node list txt generation sh
export server_nodefile_sh='aws_nodefile_server.sh'
export client_nodefile_sh='aws_nodefile_client.sh'
export client_serverlist_file='aws_serverlistfile_client.sh'
# command sh
export nodes_run_sh='nodes_run.sh'
export nodes_shutdown_sh='nodes_shutdown.sh'
export client_run_sh='client_run.sh'


tansport_server()
{
    # transport server files
    echo "Transporting files to aws server machine."
    # meta
    scp -i $key $public_key ubuntu@$server_public_ip:
    scp -i $key $info_txt ubuntu@$server_public_ip:
    # jar file
    scp -i $key $server_jar_file ubuntu@$server_public_ip:
    # env sh 
    scp -i $key $server_env_sh ubuntu@$server_public_ip:
    # txt generation sh
    scp -i $key $server_nodefile_sh ubuntu@$server_public_ip:
    # command sh 
    scp -i $key $nodes_run_sh ubuntu@$server_public_ip:
    scp -i $key $nodes_shutdown_sh ubuntu@$server_public_ip:
}

transport_client()
{
    # transport client files 
    echo "Transporting files to aws client machine."
    # meta
    scp -i $key $info_txt ubuntu@$client_public_ip:
    # jar file
    scp -i $key $server_jar_file ubuntu@$client_public_ip:
    scp -i $key $client_jar_file ubuntu@$client_public_ip:
    # env sh 
    scp -i $key $client_env_sh ubuntu@$client_public_ip:
    # txt generation sh
    scp -i $key $client_nodefile_sh ubuntu@$client_public_ip:
    scp -i $key $client_serverlist_file ubuntu@$client_public_ip:
    # command sh 
    scp -i $key $nodes_run_sh ubuntu@$client_public_ip:
    scp -i $key $nodes_shutdown_sh ubuntu@$client_public_ip:
    scp -i $key $client_run_sh ubuntu@$client_public_ip:
}

# TODO: if 2 args
if [[ $1 == ssh ]]; then
    if [[ $2 == server ]]; then
        echo "ssh to aws server machine"
        ssh -i $key ubuntu@$server_public_ip
    elif [[ $2 == client ]]; then
        echo "ssh to aws client machine"
        ssh -i $key ubuntu@$client_public_ip
fi

# TODO: if 1-2 args
if [[ $1 == transport_to ]]; then
    if [[ $2 == server ]]; then
        transport_server
    elif [[ $2 == client ]]; then
        transport_client    
    else 
        transport_server
        transport_client
    fi
fi

# TODO: if 2 args
if [[ $1 == transport_back ]]; then
    if [[ $2 == server ]]; then
        server_log=$(echo `grep -i "Server_log" $meta` | cut -d ":" -f 2)
        echo "transferring log $server_log back from aws server machine"
        scp -i $key ubuntu@$server_public_ip:$server_log $server_log
    elif [[ $2 == client ]]; then
        client_log=$(echo `grep -i "Client_log" $meta` | cut -d ":" -f 2) 
        echo "transferring log $client_log back from aws client machine"
        scp -i $key ubuntu@$client_public_ip:$client_log $client_log
    fi
fi