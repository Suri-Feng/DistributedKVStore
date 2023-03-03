#!/bin/bash
# install java
sudo apt-get update
sudo apt install openjdk-17-jre-headless
# install net-tools
sudo apt-get install net-tools
# add public key
cat cpen431_pop.pub >> ~/.ssh/authorized_keys
# generate txt files for node information
bash aws_nodefile_server.sh