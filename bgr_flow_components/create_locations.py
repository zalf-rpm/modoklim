#!/usr/bin/python
# -*- coding: UTF-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/. */

# Authors:
# Michael Berg-Mohnicke <michael.berg@zalf.de>
#
# Maintainers:
# Currently maintained by the authors.
#
# This file is part of the util library used by models created at the Institute of
# Landscape Systems Analysis at the ZALF.
# Copyright (C: Leibniz Centre for Agricultural Landscape Research (ZALF)

import capnp
import os
from pathlib import Path
import string
import sys

PATH_TO_REPO = Path(os.path.realpath(__file__)).parent.parent
if str(PATH_TO_REPO) not in sys.path:
    sys.path.insert(1, str(PATH_TO_REPO))

PATH_TO_PYTHON_CODE = PATH_TO_REPO / "../mas-infrastructure/src/python"
if str(PATH_TO_PYTHON_CODE) not in sys.path:
    sys.path.insert(1, str(PATH_TO_PYTHON_CODE))

import common.common as common
import common.geo as geo

PATH_TO_CAPNP_SCHEMAS = (PATH_TO_REPO / "../mas-infrastructure/capnproto_schemas").resolve()
abs_imports = [str(PATH_TO_CAPNP_SCHEMAS)]
common_capnp = capnp.load(str(PATH_TO_CAPNP_SCHEMAS / "common.capnp"), imports=abs_imports) 

#------------------------------------------------------------------------------

config = {
    "split_at": ",",
    "in_sr": None, # string
    "out_sr": None # utm_coord + id attr
}
common.update_config(config, sys.argv, print_config=True, allow_new_keys=False)

conman = common.ConnectionManager()
inp = conman.try_connect(config["in_sr"], cast_as=common_capnp.Channel.Reader, retry_secs=1)
outp = conman.try_connect(config["out_sr"], cast_as=common_capnp.Channel.Writer, retry_secs=1)

try:
    if inp and outp:
        while True:
            msg = inp.read().wait()
            # check for end of data from in port
            if msg.which() == "done":
                break
            
            in_ip = msg.value.as_struct(common_capnp.IP)
            s : str = in_ip.content.as_text()
            s = s.rstrip()
            vals = s.split(config["split_at"])
            x_west = float(vals[0])
            x_east = float(vals[1])
            y_north = float(vals[2])
            y_south = float(vals[3])
            id = vals[4]

            for x, hor_label in enumerate(["W", "", "E"]):
                for y, vert_label in enumerate(["S", "", "N"]):
                    utm_coord = geo.name_to_struct_instance("utm32n")
                    r = x_west + x*1000 + 500
                    h = y_south + y*1000 + 500
                    id_ = id + "_" + vert_label + hor_label
                    geo.set_xy(utm_coord, r, h)
                    
                    out_ip = common_capnp.IP.new_message(content=utm_coord)
                    common.copy_fbp_attr(in_ip, out_ip, "id", id_)
                    outp.write(value=out_ip).wait()

        # close out port
        outp.write(done=None).wait()

except Exception as e:
    print("create_locations.py ex:", e)

print("create_locations.py: exiting run")

