#!/usr/bin/python
# -*- coding: UTF-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/. */

# Authors:
# Michael Berg-Mohnicke <michael.berg-mohnicke@zalf.de>
#
# Maintainers:
# Currently maintained by the authors.
#
# This file has been created at the Institute of
# Landscape Systems Analysis at the ZALF.
# Copyright (C: Leibniz Centre for Agricultural Landscape Research (ZALF)

import socket
import subprocess as sp
import sys
import time
import uuid

def get_free_port():
    with socket.socket() as s:
        s.bind(('',0))
        return s.getsockname()[1]

config = {
    "hpc": False,
    "nodes": "3",
    "use_infiniband": False,
    "path_to_channel": "/home/berg/GitHub/mas-infrastructure/src/cpp/common/_cmake_linux_release/channel",
    "path_to_monica": "/home/berg/GitHub/monica/_cmake_linux_release/monica-capnp-fbp-component",
    "path_to_mas": "/home/berg/GitHub/mas-infrastructure",
    "path_to_klimertrag": "/home/berg/GitHub/klimertrag",
    "path_to_out_dir": "/home/berg/GitHub/klimertrag/out_fbp",
    "setups_file": "/home/berg/GitHub/klimertrag/sim_setups_bgr_flow.csv",
    "coords_file": "/home/berg/Desktop/all_coord_shuffled_anonymous.csv",
    "monica_count": "10",
    "proj_transformer_count": "5",
    "ilr_count": "5",
    "dwd_count": "5",
    "writer_count": "3",
}
if len(sys.argv) > 1 and __name__ == "__main__":
    for arg in sys.argv[1:]:
        k,v = arg.split("=")
        if k in config:
            if v.lower() in ["true", "false"]:
                config[k] = v.lower() == "true"
            else:
                config[k] = v
print(config)

node_count = int(config["nodes"])
use_infiniband = config["use_infiniband"]
node_hostname = socket.gethostname()
if config["use_infiniband"]:
    node_hostname.replace(".service", ".opa")
node_ip = socket.gethostbyname(node_hostname)
node_name = node_hostname.replace(".service", "").replace(".opa", "")

parts = []
shared_in_srt = str(uuid.uuid4())
shared_channel_port = get_free_port()
shared_in_sr = "capnp://insecure@{host}:{port}/{srt}".format(host=node_ip, port=shared_channel_port, srt=shared_in_srt),

if config["hpc"]:
    _ = sp.Popen([
        "srun",
        "-N1",
        "-n1",
        "-w{}".format(node_name),
        "-ohpc_bgr_flow_py_part_1.out",
        "-ehpc_bgr_flow_py_part_1.err",
        "python", 
        "{}/run_bgr_flow_part_1.py".format(config["path_to_klimertrag"]), 
        "hpc={}".format(config["hpc"]),
        "shared_in_srt={}".format(shared_in_srt),
        "shared_channel_port={}".format(shared_channel_port),
        "use_infiniband={}".format(config["use_infiniband"]),
        "path_to_channel={}".format(config["path_to_channel"]),
        "path_to_mas={}".format(config["path_to_mas"]),
        "setups_file={}".format(config["setups_file"]),
        "coords_file={}".format(config["coords_file"]),
    ])
else:
    _ = sp.Popen([
        "python", 
        "{}/run_bgr_flow_part_1.py".format(config["path_to_klimertrag"]), 
        "hpc={}".format(config["hpc"]),
        "shared_in_srt={}".format(shared_in_srt),
        "shared_channel_port={}".format(shared_channel_port),
        "use_infiniband={}".format(config["use_infiniband"]),
        "path_to_channel={}".format(config["path_to_channel"]),
        "path_to_mas={}".format(config["path_to_mas"]),
        "setups_file={}".format(config["setups_file"]),
        "coords_file={}".format(config["coords_file"]),
    ])
parts.append(_)
print("started part 1")

time.sleep(3)

if config["hpc"]:
    _ = sp.Popen([
        "srun",
        "-N{}".format(node_count-1),
        "-n{}".format(node_count-1),
        "-ohpc_bgr_flow_py_part_2.out",
        "-ehpc_bgr_flow_py_part_2.err",
        "python", 
        "{}/run_bgr_flow_part_2.py".format(config["path_to_klimertrag"]), 
        "hpc={}".format(config["hpc"]),
        "shared_in_sr=capnp://insecure@{host}:{port}/{srt}".format(host=node_ip, port=shared_channel_port, srt=shared_in_srt),
        "use_infiniband={}".format(config["use_infiniband"]),
        "path_to_channel={}".format(config["path_to_channel"]),
        "path_to_monica={}".format(config["path_to_monica"]),
        "path_to_mas={}".format(config["path_to_mas"]),
        "path_to_klimertrag={}".format(config["path_to_klimertrag"]),
        "path_to_out_dir={}".format(config["path_to_out_dir"]),
        "setups_file={}".format(config["setups_file"]),
        "coords_file={}".format(config["coords_file"]),
        "monica_count={}".format(config["monica_count"]),
        "proj_transformer_count={}".format(config["proj_transformer_count"]),
        "ilr_count={}".format(config["ilr_count"]),
        "dwd_count={}".format(config["dwd_count"]),
        "writer_count={}".format(config["writer_count"]),
    ])
else:
    _ = sp.Popen([
        "python", 
        "{}/run_bgr_flow_part_2.py".format(config["path_to_klimertrag"]), 
        "hpc={}".format(config["hpc"]),
        "shared_in_sr=capnp://insecure@{host}:{port}/{srt}".format(host=node_ip, port=shared_channel_port, srt=shared_in_srt),
        "use_infiniband={}".format(config["use_infiniband"]),
        "path_to_channel={}".format(config["path_to_channel"]),
        "path_to_monica={}".format(config["path_to_monica"]),
        "path_to_mas={}".format(config["path_to_mas"]),
        "path_to_klimertrag={}".format(config["path_to_klimertrag"]),
        "path_to_out_dir={}".format(config["path_to_out_dir"]),
        "setups_file={}".format(config["setups_file"]),
        "coords_file={}".format(config["coords_file"]),
        "monica_count={}".format(config["monica_count"]),
        "proj_transformer_count={}".format(config["proj_transformer_count"]),
        "ilr_count={}".format(config["ilr_count"]),
        "dwd_count={}".format(config["dwd_count"]),
        "writer_count={}".format(config["writer_count"]),
    ])
parts.append(_)
print("started part 2")

for part in parts:
    part.wait()
print("run_bgr_flow_slurm.py: all parts finished")

