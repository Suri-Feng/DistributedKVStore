meta='info.txt'
key=$(echo `grep -i "Key" $meta` | cut -d ":" -f 2) 
server_public_ip=$(echo `grep -i "Server-public-ip" $meta` | cut -d ":" -f 2) 
client_public_ip=$(echo `grep -i "Client-public-ip" $meta` | cut -d ":" -f 2) 

# transferring 
#jar 
server_jar_file=$(echo `grep -i "Server_jar_file" $meta` | cut -d ":" -f 2) 
client_jar_file=$(echo `grep -i "Client_jar_file" $meta` | cut -d ":" -f 2) 
# meta
info_txt='info.txt'
public_key='cpen431_pop.pub'
# static params 
# env setting sh
client_env_sh='aws_env_client.sh'
server_env_sh='aws_env_server.sh'
# node list txt generation sh
server_nodefile_sh='aws_nodefile_server.sh'
client_nodefile_sh='aws_nodefile_client.sh'
client_serverlist_file='aws_serverlistfile_client.sh'
# command sh
nodes_run_sh='nodes_run.sh'
nodes_shutdown_sh='nodes_shutdown.sh'

# transport server files
# meta
scp -i $key $public_key ubuntu@$server_public_ip:
scp -i $key $info_txt ubuntu@$server_public_ip:
# jar file
scp -i $key $server_jar_file ubuntu@$server_public_ip:
# env sh 
scp -i $key $env_sh ubuntu@$server_public_ip:
# txt generation sh
scp -i $key $server_nodefile_sh ubuntu@$server_public_ip:
# command sh 
scp -i $key $nodes_run_sh ubuntu@$server_public_ip:
scp -i $key $nodes_shutdown_sh ubuntu@$server_public_ip:


# transport client files 
# meta
scp -i $key $info_txt ubuntu@$client_public_ip:
# jar file
scp -i $key $server_jar_file ubuntu@$client_public_ip:
scp -i $key $client_jar_file ubuntu@$client_public_ip:
# env sh 
scp -i $key $env_sh ubuntu@$client_public_ip:
# txt generation sh
scp -i $key $client_nodefile_sh ubuntu@$client_public_ip:
scp -i $key $client_serverlist_file ubuntu@$client_public_ip:
# command sh 
scp -i $key $nodes_run_sh ubuntu@$client_public_ip:
scp -i $key $nodes_shutdown_sh ubuntu@$client_public_ip: