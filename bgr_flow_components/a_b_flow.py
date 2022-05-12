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

r1 = str(uuid.uuid4())
w1 = str(uuid.uuid4())
_ = sp.Popen([
    "python", 
    "/home/berg/GitHub/mas-infrastructure/src/python/common/channel.py", 
    "port=9991",
    "no_of_channels=1",
    "buffer_size=1",
    "reader_srts="+json.dumps([[r1]]),
    "writer_srts="+json.dumps([[w1]]),
    "use_async=True"
])
channels.append(_)

_ = sp.Popen([
    "python", 
    "/home/berg/GitHub/klimertrag/bgr_flow_components/a.py", 
    "out_sr=capnp://insecure@10.10.24.210:9991/"+w1
])
components.append(_)

_ = sp.Popen([
    "python", 
    "/home/berg/GitHub/klimertrag/bgr_flow_components/b.py", 
    "in_sr=capnp://insecure@10.10.24.210:9991/"+r1, 
])
components.append(_)

for component in components:
    component.wait()
print("run_bgr_flow.py: all components finished")

for channel in channels:
    channel.terminate()
print("run_bgr_flow.py: all channels terminated")

