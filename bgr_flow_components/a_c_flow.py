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

import json
import os
from pathlib import Path
import subprocess as sp
import time
from threading import Thread
import uuid

components = []
channels = []
rs = []
ws = []

path_to_channel = "/home/berg/GitHub/mas-infrastructure/src/cpp/common/_cmake_linux_debug/channel"

# r1 = "r1"#str(uuid.uuid4())
# w1 = "w1"#str(uuid.uuid4())
# #_ = sp.Popen([
# #    "python", 
# #    "/home/berg/GitHub/mas-infrastructure/src/python/common/channel.py", 
# #    "port=9991",
# #    "name=a-b",
# #    "no_of_channels=1",
# #    "buffer_size=1",
# #    "reader_srts="+json.dumps([[r1]]),
# #    "writer_srts="+json.dumps([[w1]]),
# #    "use_async=True"
# #])
# _ = sp.Popen([
#    "/home/berg/GitHub/mas-infrastructure/src/cpp/common/_cmake_linux_debug/channel",
#    "host=10.10.24.218",
#    "name=ab",
#    "port=9991",
#    "reader_srts="+r1,
#    "writer_srts="+w1,
# ])
# channels.append(_)

rs.append(str(uuid.uuid4()))
ws.append(str(uuid.uuid4()))
_ = sp.Popen([
   path_to_channel,
   "host=10.10.24.218",
   "name=chan1",
   "port=9991",
   "reader_srts="+rs[-1],
   "writer_srts="+ws[-1],
])
channels.append(_)

_ = sp.Popen([
    "python", 
    "/home/berg/GitHub/klimertrag/bgr_flow_components/a.py",
    "name=a",
    "out_sr=capnp://insecure@10.10.24.218:99{:02g}/{}".format(len(channels), ws[-1]),
])
components.append(_)

for i in range(2, 3):
    rs.append(str(uuid.uuid4()))
    ws.append(str(uuid.uuid4()))

    _ = sp.Popen([
    path_to_channel,
    "host=10.10.24.218",
    "name=chan"+str(i+1),
    "port=999"+str(i),
    "reader_srts="+rs[-1],
    "writer_srts="+ws[-1],
    ])
    channels.append(_)

    _ = sp.Popen([
        "python", 
        "/home/berg/GitHub/klimertrag/bgr_flow_components/b.py", 
        "name=b"+str(i-1),
        "in_sr=capnp://insecure@10.10.24.218:99{:02g}/{}".format(len(channels)-1, rs[-2]),
        "out_sr=capnp://insecure@10.10.24.218:99{:02g}/{}".format(len(channels), ws[-1]),
    ])
    components.append(_)

_ = sp.Popen([
    "python", 
    "/home/berg/GitHub/klimertrag/bgr_flow_components/c.py", 
    "name=c",
    "in_sr=capnp://insecure@10.10.24.218:99{:02g}/{}".format(len(channels), rs[-1]),
])
components.append(_)

for component in components:
    component.wait()
print("run_bgr_flow.py: all components finished")

for channel in channels:
    channel.terminate()
print("run_bgr_flow.py: all channels terminated")

