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
import uuid

def get_free_port():
    with socket.socket() as s:
        s.bind(('',0))
        return s.getsockname()[1]

def start_channel(path_to_channel, host, chan_port, reader_srt, writer_srt):
    return sp.Popen([
        path_to_channel, 
        "--host={}".format(host),
        "--name=chan_{}".format(chan_port),
        "--port={}".format(chan_port),
        "--reader_srts={}".format(reader_srt),
        "--writer_srts={}".format(writer_srt),
    ])

config = {
    "hpc": False,
    "shared_in_srt": str(uuid.uuid4()),
    "shared_channel_port": str(get_free_port()),
    "use_infiniband": False,
    "path_to_channel": "/home/berg/GitHub/mas-infrastructure/src/cpp/common/_cmake_linux_release/channel",
    "path_to_mas": "/home/berg/GitHub/mas-infrastructure",
    "setups_file": "/home/berg/GitHub/klimertrag/sim_setups_bgr_flow.csv",
    "coords_file": "/home/berg/Desktop/all_coord_shuffled_anonymous.csv",
}
if len(sys.argv) > 1 and __name__ == "__main__":
    for arg in sys.argv[1:]:
        k,v = arg.split("=")
        if k in config:
            if v.lower() in ["true", "false"]:
                config[k] = v.lower() == "true"
            else:
                config[k] = v

use_infiniband = config["use_infiniband"]
node_hostname = socket.gethostname()
node_fqdn = node_hostname + (".opa" if use_infiniband else ".service") if config["hpc"] else ""
node_ip = socket.gethostbyname(node_fqdn)

components = []
channels = []
rs = [] # reader sturdy ref tokens
ws = [] # writer sturdy ref tokens
ps = [] # ports

rs.append(str(uuid.uuid4()))
ws.append(str(uuid.uuid4()))
ps.append(get_free_port())
channels.append(start_channel(config["path_to_channel"], node_ip, ps[-1], rs[-1], ws[-1]))

_ = sp.Popen([
    "python", 
    "{}/src/python/fbp/read_csv.py".format(config["path_to_mas"]), 
    "out_sr=capnp://insecure@{host}:{port}/{srt}".format(host=node_ip, port=ps[-1], srt=ws[-1]),
    "file="+config["setups_file"],
    "path_to_capnp_struct=bgr.capnp:Setup",
    "id_col=runId",
    "send_ids=1",
])
components.append(_)

rs.append(config["shared_in_srt"])
ws.append(str(uuid.uuid4()))
ps.append(int(config["shared_channel_port"]))
channels.append(start_channel(config["path_to_channel"], node_ip, ps[-1], rs[-1], ws[-1]))

channels.append(_)
_ = sp.Popen([
    "python", 
    "{}/src/python/fbp/read_file.py".format(config["path_to_mas"]), 
    "attr_sr=capnp://insecure@{host}:{port}/{srt}".format(host=node_ip, port=ps[-2], srt=rs[-2]),
    "to_attr=setup",
    "out_sr=capnp://insecure@{host}:{port}/{srt}".format(host=node_ip, port=ps[-1], srt=ws[-1]),
    "skip_lines=1",
    "file="+config["coords_file"]
])
components.append(_)

#-----------------------------------------------------------------------------

for component in components:
    component.wait()
print("run_bgr_flow_part_1.py: all components finished")

for channel in channels:
    channel.terminate()
print("run_bgr_flow_part_1.py: all channels terminated")

