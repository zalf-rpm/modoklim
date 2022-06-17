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
    "shared_in_sr": "",
    "use_infiniband": False,
    "path_to_channel": "/home/berg/GitHub/mas-infrastructure/src/cpp/common/_cmake_linux_release/channel",
    "path_to_monica": "/home/berg/GitHub/monica/_cmake_linux_release/monica-capnp-fbp-component",
    "path_to_mas": "/home/berg/GitHub/mas-infrastructure",
    "path_to_klimertrag": "/home/berg/GitHub/klimertrag",
    "path_to_out_dir": "/home/berg/GitHub/klimertrag/fbp_out",
    "path_to_dwd_csvs": "/run/user/1000/gvfs/sftp:host=login01.cluster.zalf.de,user=rpm/beegfs/common/data/climate/dwd/csvs",
    "setups_file": "/home/berg/GitHub/mas-infrastructure/sim_setups_bgr_flow.csv",
    "coords_file": "/home/berg/Desktop/all_coord_shuffled_anonymous.csv",
    "monica_count": "5",
    "proj_transformer_count": "3",
    "ilr_count": "3",
    "dwd_count": "3",
    "writer_count": "1",

}
if len(sys.argv) > 1 and __name__ == "__main__":
    for arg in sys.argv[1:]:
        k,v = arg.split("=", maxsplit=1)
        if k in config:
            if v.lower() in ["true", "false"]:
                config[k] = v.lower() == "true"
            else:
                config[k] = v
print(config)

use_infiniband = config["use_infiniband"]
node_hostname = socket.gethostname()
if config["use_infiniband"]:
    node_hostname.replace(".service", ".opa")
node_ip = socket.gethostbyname(node_hostname)

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
    "{}/bgr_flow_components/create_locations.py".format(config["path_to_klimertrag"]), 
    "in_sr={}".format(config["shared_in_sr"]),
    "out_sr=capnp://insecure@{host}:{port}/{srt}".format(host=node_ip, port=ps[-1], srt=ws[-1]),
])
components.append(_)

rs.append(str(uuid.uuid4()))
ws.append(str(uuid.uuid4()))
ps.append(get_free_port())
channels.append(start_channel(config["path_to_channel"], node_ip, ps[-1], rs[-1], ws[-1]))

for _ in range(int(config["proj_transformer_count"])):
    _ = sp.Popen([
        "python", 
        "{}/src/python/fbp/proj_transformer.py".format(config["path_to_mas"]), 
        "in_sr=capnp://insecure@{host}:{port}/{srt}".format(host=node_ip, port=ps[-2], srt=rs[-2]),
        "out_sr=capnp://insecure@{host}:{port}/{srt}".format(host=node_ip, port=ps[-1], srt=ws[-1]),
        "to_attr=latlon",
        "from_name=utm32n",
        "to_name=latlon",
    ])
    components.append(_)

rs.append(str(uuid.uuid4()))
ws.append(str(uuid.uuid4()))
ps.append(get_free_port())
channels.append(start_channel(config["path_to_channel"], node_ip, ps[-1], rs[-1], ws[-1]))

_ = sp.Popen([
    "python", 
    "{}/src/python/fbp/lift_attributes.py".format(config["path_to_mas"]), 
    "in_sr=capnp://insecure@{host}:{port}/{srt}".format(host=node_ip, port=ps[-2], srt=rs[-2]),
    "out_sr=capnp://insecure@{host}:{port}/{srt}".format(host=node_ip, port=ps[-1], srt=ws[-1]),
    "lift_from_attr=setup",
    "lift_from_type=bgr.capnp:Setup",
    "lifted_attrs=sowingTime,harvestTime,cropId,startDate,endDate",
])
components.append(_)

rs.append(str(uuid.uuid4()))
ws.append(str(uuid.uuid4()))
ps.append(get_free_port())
channels.append(start_channel(config["path_to_channel"], node_ip, ps[-1], rs[-1], ws[-1]))

for _ in range(int(config["ilr_count"])):
    _ = sp.Popen([
        "python", 
        "{}/src/python/services/management/ilr_sowing_harvest_dates_fbp_component.py".format(config["path_to_mas"]), 
        "in_sr=capnp://insecure@{host}:{port}/{srt}".format(host=node_ip, port=ps[-2], srt=rs[-2]),
        "out_sr=capnp://insecure@{host}:{port}/{srt}".format(host=node_ip, port=ps[-1], srt=ws[-1]),
        "latlon_attr=latlon",
        "crop_id_attr=cropId",
        "sowing_time_attr=sowingTime",
        "harvest_time_attr=harvestTime", 
        "to_attr=ilr",
    ])
    components.append(_)

rs.append(str(uuid.uuid4()))
ws.append(str(uuid.uuid4()))
ps.append(get_free_port())
channels.append(start_channel(config["path_to_channel"], node_ip, ps[-1], rs[-1], ws[-1]))

_ = sp.Popen([
    "python", 
    "{}/src/python/services/soil/sqlite_soil_data_service.py".format(config["path_to_mas"]), 
    "in_sr=capnp://insecure@{host}:{port}/{srt}".format(host=node_ip, port=ps[-2], srt=rs[-2]),
    "out_sr=capnp://insecure@{host}:{port}/{srt}".format(host=node_ip, port=ps[-1], srt=ws[-1]),
    "path_to_sqlite_db={}/data/soil/buek200.sqlite".format(config["path_to_mas"]),
    "path_to_ascii_soil_grid={}/data/soil/buek200_1000_25832_etrs89-utm32n.asc".format(config["path_to_mas"]),
    "fbp=true",
    """mandatory=["soilType","organicCarbon","rawDensity"]""",
    "from_attr=latlon",
    "to_attr=soil"
])
components.append(_)

rs.append(str(uuid.uuid4()))
ws.append(str(uuid.uuid4()))
ps.append(get_free_port())
channels.append(start_channel(config["path_to_channel"], node_ip, ps[-1], rs[-1], ws[-1]))

_ = sp.Popen([
    "python", 
    "{}/src/python/services/grid/ascii_grid.py".format(config["path_to_mas"]), 
    "in_sr=capnp://insecure@{host}:{port}/{srt}".format(host=node_ip, port=ps[-2], srt=rs[-2]),
    "out_sr=capnp://insecure@{host}:{port}/{srt}".format(host=node_ip, port=ps[-1], srt=ws[-1]),
    "path_to_ascii_grid={}/data/geo/dem_1000_31469_gk5.asc".format(config["path_to_mas"]),
    "grid_crs=gk5",
    "val_type=float",
    "fbp=true",
    "from_attr=latlon",
    "to_attr=dgm"
])
components.append(_)

rs.append(str(uuid.uuid4()))
ws.append(str(uuid.uuid4()))
ps.append(get_free_port())
channels.append(start_channel(config["path_to_channel"], node_ip, ps[-1], rs[-1], ws[-1]))

_ = sp.Popen([
    "python", 
    "{}/src/python/services/grid/ascii_grid.py".format(config["path_to_mas"]), 
    "in_sr=capnp://insecure@{host}:{port}/{srt}".format(host=node_ip, port=ps[-2], srt=rs[-2]),
    "out_sr=capnp://insecure@{host}:{port}/{srt}".format(host=node_ip, port=ps[-1], srt=ws[-1]),
    "path_to_ascii_grid={}/data/geo/slope_1000_31469_gk5.asc".format(config["path_to_mas"]),
    "grid_crs=gk5",
    "val_type=float",
    "fbp=true",
    "from_attr=latlon",
    "to_attr=slope"
])
components.append(_)

rs.append(str(uuid.uuid4()))
ws.append(str(uuid.uuid4()))
ps.append(get_free_port())
channels.append(start_channel(config["path_to_channel"], node_ip, ps[-1], rs[-1], ws[-1]))

for _ in range(int(config["dwd_count"])):
    _ = sp.Popen([
        "python", 
        "{}/src/python/services/climate/dwd_germany_service.py".format(config["path_to_mas"]), 
        "in_sr=capnp://insecure@{host}:{port}/{srt}".format(host=node_ip, port=ps[-2], srt=rs[-2]),
        "out_sr=capnp://insecure@{host}:{port}/{srt}".format(host=node_ip, port=ps[-1], srt=ws[-1]),
        "fbp=true",
        "latlon_attr=latlon",
        "start_date_attr=startDate",
        "end_date_attr=endDate",
        "to_attr=climate",
        "mode=capability",
        "path_to_data={}".format(config["path_to_dwd_csvs"]),
    ])
    components.append(_)

rs.append(str(uuid.uuid4()))
ws.append(str(uuid.uuid4()))
ps.append(get_free_port())
channels.append(start_channel(config["path_to_channel"], node_ip, ps[-1], rs[-1], ws[-1]))

_ = sp.Popen([
    "python", 
    "{}/bgr_flow_components/create_bgr_env.py".format(config["path_to_klimertrag"]), 
    "in_sr=capnp://insecure@{host}:{port}/{srt}".format(host=node_ip, port=ps[-2], srt=rs[-2]),
    "out_sr=capnp://insecure@{host}:{port}/{srt}".format(host=node_ip, port=ps[-1], srt=ws[-1]),
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
ps.append(get_free_port())
channels.append(start_channel(config["path_to_channel"], node_ip, ps[-1], rs[-1], ws[-1]))

for _ in range(int(config["monica_count"])):
    _ = sp.Popen([
        config["path_to_monica"],
        "--in_sr=capnp://insecure@{host}:{port}/{srt}".format(host=node_ip, port=ps[-2], srt=rs[-2]),
        "--out_sr=capnp://insecure@{host}:{port}/{srt}".format(host=node_ip, port=ps[-1], srt=ws[-1]),
    ])
    components.append(_)

rs.append(str(uuid.uuid4()))
ws.append(str(uuid.uuid4()))
ps.append(get_free_port())
channels.append(start_channel(config["path_to_channel"], node_ip, ps[-1], rs[-1], ws[-1]))

_ = sp.Popen([
    "python", 
    "{}/bgr_flow_components/create_out_path.py".format(config["path_to_klimertrag"]), 
    "in_sr=capnp://insecure@{host}:{port}/{srt}".format(host=node_ip, port=ps[-2], srt=rs[-2]),
    "out_sr=capnp://insecure@{host}:{port}/{srt}".format(host=node_ip, port=ps[-1], srt=ws[-1]),
    "id_attr=id",
    "to_attr=out_path",
    "bin_size=1000",
    "dir_template={}/{}".format(config["path_to_out_dir"], "{}"),
])
components.append(_)

for _ in range(int(config["writer_count"])):
    _ = sp.Popen([
        "python", 
        "{}/src/python/fbp/write_monica_csv.py".format(config["path_to_mas"]), 
        "in_sr=capnp://insecure@{host}:{port}/{srt}".format(host=node_ip, port=ps[-1], srt=rs[-1]),
        "file_pattern=csv_{id}.csv",
        "id_attr=id",
        "out_path_attr=out_path",
    ])
    components.append(_)

for component in components:
    component.wait()
print("run_bgr_flow_part_2.py: all components finished")

for channel in channels:
    channel.terminate()
print("run_bgr_flow_part_2.py: all channels terminated")

