#!/bin/bash

if [[ $1 == setup ]]; then
  # install java
  sudo apt-get update
  sudo apt install openjdk-17-jre-headless
  # install net-tools
  sudo apt-get install net-tools

  public_key="cpen431_pop.pub"
  if [[ -f "$public_key" ]]; then
    # add public key if exists (server port)
    cat $public_key >> ~/.ssh/authorized_keys
  fi
elif [[ $1 == del_netem ]]; then
  #sudo tc qdisc del dev <iface> root
  sudo tc qdisc del dev lo root
  sudo tc qdisc del dev ens5 root
elif [[ $1 ==  set_netem ]]; then
  #TODO: check arguments
  sudo tc qdisc add dev lo   root netem delay 5msec loss 2.5%
  sudo tc qdisc add dev ens5 root netem delay 5msec loss 2.5%
fi

