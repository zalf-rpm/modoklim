#!/bin/bash

echo "running with " $1 " number of processes" 

for i in $(seq 1 $1)
do
    #echo $i
    python run_capnp_bgr.py &
done