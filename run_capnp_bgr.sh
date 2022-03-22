#!/bin/bash

echo "running with " $1 " number of processes, getting config from " $2 

for i in $(seq 1 $1)
do
    #echo $i
    python run_capnp_bgr.py config_sr=$2 &
done