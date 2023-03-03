#!/bin/bash
# install java
sudo apt-get update
sudo apt install openjdk-17-jre-headless
# install net-tools
sudo apt-get install net-tools
# generate txt files for node information
bash aws_nodefile_client.sh
bash aws_serverlistfile_client.sh