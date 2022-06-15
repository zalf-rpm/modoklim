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

path_to_channel = "/home/berg/GitHub/mas-infrastructure/src/cpp/common/_cmake_linux_release/channel"
host = "10.10.24.218"
rs = []
ws = []

rs.append(str(uuid.uuid4()))
ws.append(str(uuid.uuid4()))
_ = sp.Popen([
    path_to_channel, 
    "--host={}".format(host),
    "--name=chan_{}".format(len(channels)+1),
    "--port=9{:03g}".format(len(channels)+1),
    "--reader_srts="+rs[-1],
    "--writer_srts="+ws[-1],
])
channels.append(_)

_ = sp.Popen([
    "python", 
    "/home/berg/GitHub/mas-infrastructure/src/python/fbp/read_csv.py", 
    "out_sr=capnp://insecure@{host}:9{port:03g}/{srt}".format(host=host, port=len(channels), srt=ws[-1]),
    "file=sim_setups_bgr_flow.csv",
    "path_to_capnp_struct=bgr.capnp:Setup",
    "id_col=runId",
    "send_ids=1",
])
components.append(_)

rs.append(str(uuid.uuid4()))
ws.append(str(uuid.uuid4()))
_ = sp.Popen([
    path_to_channel, 
    "--host={}".format(host),
    "--name=chan_{}".format(len(channels)+1),
    "--port=9{:03g}".format(len(channels)+1),
    "--reader_srts="+rs[-1],
    "--writer_srts="+ws[-1],
])

channels.append(_)
_ = sp.Popen([
    "python", 
    "/home/berg/GitHub/mas-infrastructure/src/python/fbp/read_file.py", 
    "attr_sr=capnp://insecure@{host}:9{port:03g}/{srt}".format(host=host, port=len(channels)-1, srt=rs[-2]),
    "to_attr=setup",
    "out_sr=capnp://insecure@{host}:9{port:03g}/{srt}".format(host=host, port=len(channels), srt=ws[-1]),
    "skip_lines=1",
    #"file=/home/berg/Desktop/Koordinaten_HE_dummy_ID.csv"
    "file=/home/berg/Desktop/all_coord_shuffled_anonymous.csv"
])
components.append(_)

rin = rs[-1]
cin = len(channels)

for _ in range(20):

    rs.append(str(uuid.uuid4()))
    ws.append(str(uuid.uuid4()))
    _ = sp.Popen([
        path_to_channel, 
        "--host={}".format(host),
        "--name=chan_{}".format(len(channels)+1),
        "--port=9{:03g}".format(len(channels)+1),
        "--reader_srts="+rs[-1],
        "--writer_srts="+ws[-1],
    ])
    channels.append(_)

    _ = sp.Popen([
        "python", 
        "/home/berg/GitHub/klimertrag/bgr_flow_components/create_locations.py", 
        "in_sr=capnp://insecure@{host}:9{port:03g}/{srt}".format(host=host, port=cin, srt=rin),
        "out_sr=capnp://insecure@{host}:9{port:03g}/{srt}".format(host=host, port=len(channels), srt=ws[-1]),
    ])
    components.append(_)

    rs.append(str(uuid.uuid4()))
    ws.append(str(uuid.uuid4()))
    _ = sp.Popen([
        path_to_channel, 
        "--host={}".format(host),
        "--name=chan_{}".format(len(channels)+1),
        "--port=9{:03g}".format(len(channels)+1),
        "--reader_srts="+rs[-1],
        "--writer_srts="+ws[-1],
    ])
    channels.append(_)

    for _ in range(1):
        _ = sp.Popen([
            "python", 
            "/home/berg/GitHub/mas-infrastructure/src/python/fbp/proj_transformer.py", 
            "in_sr=capnp://insecure@{host}:9{port:03g}/{srt}".format(host=host, port=len(channels)-1, srt=rs[-2]),
            "out_sr=capnp://insecure@{host}:9{port:03g}/{srt}".format(host=host, port=len(channels), srt=ws[-1]),
            "to_attr=latlon",
            "from_name=utm32n",
            "to_name=latlon",
        ])
        components.append(_)

    rs.append(str(uuid.uuid4()))
    ws.append(str(uuid.uuid4()))
    _ = sp.Popen([
        path_to_channel, 
        "--host={}".format(host),
        "--name=chan_{}".format(len(channels)+1),
        "--port=9{:03g}".format(len(channels)+1),
        "--reader_srts="+rs[-1],
        "--writer_srts="+ws[-1],
    ])
    channels.append(_)

    _ = sp.Popen([
        "python", 
        "/home/berg/GitHub/mas-infrastructure/src/python/fbp/lift_attributes.py", 
        "in_sr=capnp://insecure@{host}:9{port:03g}/{srt}".format(host=host, port=len(channels)-1, srt=rs[-2]),
        "out_sr=capnp://insecure@{host}:9{port:03g}/{srt}".format(host=host, port=len(channels), srt=ws[-1]),
        "lift_from_attr=setup",
        "lift_from_type=bgr.capnp:Setup",
        "lifted_attrs=sowingTime,harvestTime,cropId,startDate,endDate",
    ])
    components.append(_)

    rs.append(str(uuid.uuid4()))
    ws.append(str(uuid.uuid4()))
    _ = sp.Popen([
        path_to_channel, 
        "--host={}".format(host),
        "--name=chan_{}".format(len(channels)+1),
        "--port=9{:03g}".format(len(channels)+1),
        "--reader_srts="+rs[-1],
        "--writer_srts="+ws[-1],
    ])
    channels.append(_)

    for _ in range(1):
        _ = sp.Popen([
            "python", 
            "/home/berg/GitHub/mas-infrastructure/src/python/services/management/ilr_sowing_harvest_dates_fbp_component.py", 
            "in_sr=capnp://insecure@{host}:9{port:03g}/{srt}".format(host=host, port=len(channels)-1, srt=rs[-2]),
            "out_sr=capnp://insecure@{host}:9{port:03g}/{srt}".format(host=host, port=len(channels), srt=ws[-1]),
            "latlon_attr=latlon",
            "crop_id_attr=cropId",
            "sowing_time_attr=sowingTime",
            "harvest_time_attr=harvestTime", 
            "to_attr=ilr",
        ])
        components.append(_)


    rs.append(str(uuid.uuid4()))
    ws.append(str(uuid.uuid4()))
    _ = sp.Popen([
        path_to_channel, 
        "--host={}".format(host),
        "--name=chan_{}".format(len(channels)+1),
        "--port=9{:03g}".format(len(channels)+1),
        "--reader_srts="+rs[-1],
        "--writer_srts="+ws[-1],
    ])
    channels.append(_)

    _ = sp.Popen([
        "python", 
        "/home/berg/GitHub/mas-infrastructure/src/python/services/soil/sqlite_soil_data_service.py", 
        "in_sr=capnp://insecure@{host}:9{port:03g}/{srt}".format(host=host, port=len(channels)-1, srt=rs[-2]),
        "out_sr=capnp://insecure@{host}:9{port:03g}/{srt}".format(host=host, port=len(channels), srt=ws[-1]),
        "path_to_sqlite_db=/home/berg/GitHub/mas-infrastructure/data/soil/buek200.sqlite",
        "path_to_ascii_soil_grid=/home/berg/GitHub/mas-infrastructure/data/soil/buek200_1000_25832_etrs89-utm32n.asc",
        "fbp=true",
        """mandatory=["soilType","organicCarbon","rawDensity"]""",
        "from_attr=latlon",
        "to_attr=soil"
    ])
    components.append(_)

    rs.append(str(uuid.uuid4()))
    ws.append(str(uuid.uuid4()))
    _ = sp.Popen([
        path_to_channel, 
        "--host={}".format(host),
        "--name=chan_{}".format(len(channels)+1),
        "--port=9{:03g}".format(len(channels)+1),
        "--reader_srts="+rs[-1],
        "--writer_srts="+ws[-1],
    ])
    channels.append(_)

    _ = sp.Popen([
        "python", 
        "/home/berg/GitHub/mas-infrastructure/src/python/services/grid/ascii_grid.py", 
        "in_sr=capnp://insecure@{host}:9{port:03g}/{srt}".format(host=host, port=len(channels)-1, srt=rs[-2]),
        "out_sr=capnp://insecure@{host}:9{port:03g}/{srt}".format(host=host, port=len(channels), srt=ws[-1]),
        "path_to_ascii_grid=/home/berg/GitHub/mas-infrastructure/data/geo/dem_1000_31469_gk5.asc",
        "grid_crs=gk5",
        "val_type=float",
        "fbp=true",
        "from_attr=latlon",
        "to_attr=dgm"
    ])
    components.append(_)

    rs.append(str(uuid.uuid4()))
    ws.append(str(uuid.uuid4()))
    _ = sp.Popen([
        path_to_channel, 
        "--host={}".format(host),
        "--name=chan_{}".format(len(channels)+1),
        "--port=9{:03g}".format(len(channels)+1),
        "--reader_srts="+rs[-1],
        "--writer_srts="+ws[-1],
    ])
    channels.append(_)

    _ = sp.Popen([
        "python", 
        "/home/berg/GitHub/mas-infrastructure/src/python/services/grid/ascii_grid.py", 
        "in_sr=capnp://insecure@{host}:9{port:03g}/{srt}".format(host=host, port=len(channels)-1, srt=rs[-2]),
        "out_sr=capnp://insecure@{host}:9{port:03g}/{srt}".format(host=host, port=len(channels), srt=ws[-1]),
        "path_to_ascii_grid=/home/berg/GitHub/mas-infrastructure/data/geo/slope_1000_31469_gk5.asc",
        "grid_crs=gk5",
        "val_type=float",
        "fbp=true",
        "from_attr=latlon",
        "to_attr=slope"
    ])
    components.append(_)

    rs.append(str(uuid.uuid4()))
    ws.append(str(uuid.uuid4()))
    _ = sp.Popen([
        path_to_channel, 
        "--host={}".format(host),
        "--name=chan_{}".format(len(channels)+1),
        "--port=9{:03g}".format(len(channels)+1),
        "--reader_srts="+rs[-1],
        "--writer_srts="+ws[-1],
    ])
    channels.append(_)

    for _ in range(1):
        _ = sp.Popen([
            "python", 
            "/home/berg/GitHub/mas-infrastructure/src/python/services/climate/dwd_germany_service.py", 
            "in_sr=capnp://insecure@{host}:9{port:03g}/{srt}".format(host=host, port=len(channels)-1, srt=rs[-2]),
            "out_sr=capnp://insecure@{host}:9{port:03g}/{srt}".format(host=host, port=len(channels), srt=ws[-1]),
            "fbp=true",
            "latlon_attr=latlon",
            "start_date_attr=startDate",
            "end_date_attr=endDate",
            "to_attr=climate",
            "mode=capability"
        ])
        components.append(_)

    rs.append(str(uuid.uuid4()))
    #rs.append("monica_in")
    ws.append(str(uuid.uuid4()))
    _ = sp.Popen([
        path_to_channel, 
        "--host={}".format(host),
        "--name=chan_{}".format(len(channels)+1),
        "--port=9{:03g}".format(len(channels)+1),
        "--reader_srts="+rs[-1],
        "--writer_srts="+ws[-1],
    ])
    channels.append(_)

    _ = sp.Popen([
        "python", 
        "/home/berg/GitHub/klimertrag/bgr_flow_components/create_bgr_env.py", 
        "in_sr=capnp://insecure@{host}:9{port:03g}/{srt}".format(host=host, port=len(channels)-1, srt=rs[-2]),
        "out_sr=capnp://insecure@{host}:9{port:03g}/{srt}".format(host=host, port=len(channels), srt=ws[-1]),
        "dgm_attr=dgm",
        "slope_attr=slope",
        "climate_attr=climate",
        "soil_attr=soil",
        "coord_attr=latlon",
        "setup_attr=setup",
        "id_attr=id",
        "ilr_attr=ilr",
    ])
    components.append(_)

    rs.append(str(uuid.uuid4()))
    ws.append(str(uuid.uuid4()))
    #ws.append("monica_out")
    _ = sp.Popen([
        path_to_channel, 
        "--host={}".format(host),
        "--name=chan_{}".format(len(channels)+1),
        "--port=9{:03g}".format(len(channels)+1),
        "--reader_srts="+rs[-1],
        "--writer_srts="+ws[-1],
    ])
    channels.append(_)

    for _ in range(1):
        _ = sp.Popen([
        "/home/berg/GitHub/monica/_cmake_linux_release/monica-capnp-fbp-component", 
        "--in_sr=capnp://insecure@{host}:9{port:03g}/{srt}".format(host=host, port=len(channels)-1, srt=rs[-2]),
        "--out_sr=capnp://insecure@{host}:9{port:03g}/{srt}".format(host=host, port=len(channels), srt=ws[-1]),
        ])
        components.append(_)

    for _ in range(1):
        _ = sp.Popen([
            "python", 
            "/home/berg/GitHub/mas-infrastructure/src/python/fbp/write_monica_csv.py", 
            "in_sr=capnp://insecure@{host}:9{port:03g}/{srt}".format(host=host, port=len(channels), srt=rs[-1]),
            "path_to_out_dir=out_fbp/",
            "file_pattern=csv_{id}.csv",
            "id_attr=id"
        ])
        components.append(_)

for component in components:
    component.wait()
print("run_bgr_flow.py: all components finished")

for channel in channels:
    channel.terminate()
print("run_bgr_flow.py: all channels terminated")

