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
    "/home/berg/GitHub/mas-infrastructure/src/python/fbp/read_file.py", 
    "out_sr=capnp://insecure@10.10.24.210:9991/"+w1,
    "skip_lines=1",
    #"file=/home/berg/Desktop/Koordinaten_HE_dummy_ID.csv"
    "file=/home/berg/Desktop/all_coord_shuffled_anonymous.csv"
])
components.append(_)

r2 = str(uuid.uuid4())
w2 = str(uuid.uuid4())
_ = sp.Popen([
    "python", 
    "/home/berg/GitHub/mas-infrastructure/src/python/common/channel.py", 
    "port=9992",
    "no_of_channels=1",
    "buffer_size=1",
    "reader_srts="+json.dumps([[r2]]),
    "writer_srts="+json.dumps([[w2]]),
    "use_async=True"
])
channels.append(_)

_ = sp.Popen([
    "python", 
    "/home/berg/GitHub/klimertrag/bgr_flow_components/create_locations.py", 
    "in_sr=capnp://insecure@10.10.24.210:9991/"+r1, 
    "out_sr=capnp://insecure@10.10.24.210:9992/"+w2
])
components.append(_)

r3 = str(uuid.uuid4())
w3 = str(uuid.uuid4())
_ = sp.Popen([
    "python", 
    "/home/berg/GitHub/mas-infrastructure/src/python/common/channel.py", 
    "port=9993",
    "no_of_channels=1",
    "buffer_size=1",
    "reader_srts="+json.dumps([[r3]]),
    "writer_srts="+json.dumps([[w3]]),
    "use_async=True"
])
channels.append(_)

_ = sp.Popen([
    "python", 
    "/home/berg/GitHub/mas-infrastructure/src/python/fbp/proj_transformer.py", 
    "in_sr=capnp://insecure@10.10.24.210:9992/"+r2, 
    "out_sr=capnp://insecure@10.10.24.210:9993/"+w3,
    "to_attr=latlon",
    "to_name=utm32n"
])
components.append(_)

r4 = str(uuid.uuid4())
w4 = str(uuid.uuid4())
_ = sp.Popen([
    "python", 
    "/home/berg/GitHub/mas-infrastructure/src/python/common/channel.py", 
    "port=9994",
    "no_of_channels=1",
    "buffer_size=1",
    "reader_srts="+json.dumps([[r4]]),
    "writer_srts="+json.dumps([[w4]]),
    "use_async=True"
])
channels.append(_)

_ = sp.Popen([
    "python", 
    "/home/berg/GitHub/mas-infrastructure/src/python/services/climate/dwd_germany_service.py", 
    "in_sr=capnp://insecure@10.10.24.210:9993/"+r3, 
    "out_sr=capnp://insecure@10.10.24.210:9994/"+w4,
    "fbp=true",
    "from_attr=latlon",
    "to_attr=climate",
    "mode=capability"
])
components.append(_)

r5 = str(uuid.uuid4())
w5 = str(uuid.uuid4())
_ = sp.Popen([
    "python", 
    "/home/berg/GitHub/mas-infrastructure/src/python/common/channel.py", 
    "port=9995",
    "no_of_channels=1",
    "buffer_size=1",
    "reader_srts="+json.dumps([[r5]]),
    "writer_srts="+json.dumps([[w5]]),
    "use_async=True"
])
channels.append(_)

_ = sp.Popen([
    "python", 
    "/home/berg/GitHub/mas-infrastructure/src/python/services/soil/sqlite_soil_data_service.py", 
    "in_sr=capnp://insecure@10.10.24.210:9994/"+r4,
    "out_sr=capnp://insecure@10.10.24.210:9995/"+w5,
    "path_to_sqlite_db=/home/berg/GitHub/mas-infrastructure/data/soil/buek200.sqlite",
    "path_to_ascii_soil_grid=/home/berg/GitHub/mas-infrastructure/data/soil/buek200_1000_25832_etrs89-utm32n.asc",
    "fbp=true",
    """mandatory=["soilType","organicCarbon","rawDensity"]""",
    "from_attr=latlon",
    "to_attr=soil"
])
components.append(_)

r6 = str(uuid.uuid4())
w6 = str(uuid.uuid4())
_ = sp.Popen([
    "python", 
    "/home/berg/GitHub/mas-infrastructure/src/python/common/channel.py", 
    "port=9996",
    "no_of_channels=1",
    "buffer_size=1",
    "reader_srts="+json.dumps([[r6]]),
    "writer_srts="+json.dumps([[w6]]),
    "use_async=True"
])
channels.append(_)

_ = sp.Popen([
    "python", 
    "/home/berg/GitHub/mas-infrastructure/src/python/services/grid/ascii_grid.py", 
    "in_sr=capnp://insecure@10.10.24.210:9995/"+r5,
    "out_sr=capnp://insecure@10.10.24.210:9996/"+w6,
    "path_to_ascii_grid=/home/berg/GitHub/mas-infrastructure/data/geo/dem_1000_31469_gk5.asc",
    "grid_crs=gk5",
    "val_type=float",
    "fbp=true",
    "from_attr=latlon",
    "to_attr=dgm"
])
components.append(_)

r7 = str(uuid.uuid4())
w7 = str(uuid.uuid4())
_ = sp.Popen([
    "python", 
    "/home/berg/GitHub/mas-infrastructure/src/python/common/channel.py", 
    "port=9997",
    "no_of_channels=1",
    "buffer_size=1",
    "reader_srts="+json.dumps([[r7]]),
    "writer_srts="+json.dumps([[w7]]),
    "use_async=True"
])
channels.append(_)

_ = sp.Popen([
    "python", 
    "/home/berg/GitHub/mas-infrastructure/src/python/services/grid/ascii_grid.py", 
    "in_sr=capnp://insecure@10.10.24.210:9996/"+r6,
    "out_sr=capnp://insecure@10.10.24.210:9997/"+w7,
    "path_to_ascii_grid=/home/berg/GitHub/mas-infrastructure/data/geo/slope_1000_31469_gk5.asc",
    "grid_crs=gk5",
    "val_type=float",
    "fbp=true",
    "from_attr=latlon",
    "to_attr=dgm"
])
components.append(_)

r8 = "88888888"#str(uuid.uuid4())
w8 = str(uuid.uuid4())
_ = sp.Popen([
    "python", 
    "/home/berg/GitHub/mas-infrastructure/src/python/common/channel.py", 
    "port=9998",
    "no_of_channels=1",
    "buffer_size=1",
    "reader_srts="+json.dumps([[r8]]),
    "writer_srts="+json.dumps([[w8]]),
    "use_async=True"
])
channels.append(_)

_ = sp.Popen([
    "python", 
    "/home/berg/GitHub/klimertrag/bgr_flow_components/create_bgr_env.py", 
    "in_sr=capnp://insecure@10.10.24.210:9997/"+r7, 
    "out_sr=capnp://insecure@10.10.24.210:9998/"+w8
])
components.append(_)

r9 = str(uuid.uuid4())
w9 = "999999999"#str(uuid.uuid4())
_ = sp.Popen([
    "python", 
    "/home/berg/GitHub/mas-infrastructure/src/python/common/channel.py", 
    "port=9999",
    "no_of_channels=1",
    "buffer_size=1",
    "reader_srts="+json.dumps([[r9]]),
    "writer_srts="+json.dumps([[w9]]),
    "use_async=True"
])
channels.append(_)

#_ = sp.Popen([
#    "python", 
#    "/home/berg/GitHub/monica/_cmake_linux_debug/monica-capnp-fbp-component", 
#    "--in_sr", "capnp://insecure@10.10.24.210:9998/"+r8, 
#    "--out_sr", "capnp://insecure@10.10.24.210:9999/"+w9
#])
#components.append(_)

_ = sp.Popen([
    "python", 
    "/home/berg/GitHub/mas-infrastructure/src/python/fbp/write_monica_csv.py", 
    "in_sr=capnp://insecure@10.10.24.210:9999/"+r9,
    "filepath_pattern=out_fbp/csv_{id}.csv",
    "id_attr=id"
])
components.append(_)

for component in components:
    component.wait()
print("run_bgr_flow.py: all components finished")

for channel in channels:
    channel.terminate()
print("run_bgr_flow.py: all channels terminated")

