#!/bin/bash
#TODO check arg

# install java
sudo apt-get update
sudo apt install openjdk-17-jre-headless
# install net-tools
sudo apt-get install net-tools

if [[ $1 == server ]]; then
  # add public key
  cat cpen431_pop.pub >> ~/.ssh/authorized_keys
  # generate txt files for node information
  bash aws_nodefile_server.sh
elif [[ $1 == client ]]; then
  # generate txt files for node information
  bash aws_nodefile_client.sh
  bash aws_serverlistfile_client.sh
fi


if [[ $1 == del_netem ]]; then
  #sudo tc qdisc del dev <iface> root
  sudo tc qdisc del dev lo root
  sudo tc qdisc del dev ens5 root
fi
#TODO 1 = set_netem
sudo tc qdisc add dev lo   root netem delay 5msec loss 2.5%
sudo tc qdisc add dev ens5 root netem delay 5msec loss 2.5%
