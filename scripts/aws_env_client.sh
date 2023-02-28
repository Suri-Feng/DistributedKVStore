# install java
sudo apt-get update
sudo apt install openjdk-17-jre-headless
# emulate network latency
sudo tc qdisc add dev lo   root netem delay 5msec loss 2.5%
sudo tc qdisc add dev ens5 root netem delay 5msec loss 2.5%
# generate txt files for node information
bash aws_nodefile_client.sh
bash aws_serverlistfile_client.sh