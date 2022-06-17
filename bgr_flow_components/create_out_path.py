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
# This file has been created at the Institute of
# Landscape Systems Analysis at the ZALF.
# Copyright (C: Leibniz Centre for Agricultural Landscape Research (ZALF)

import capnp
from collections import defaultdict
import csv
from datetime import date, timedelta
import json
import os
from pathlib import Path
from pyproj import CRS, Transformer
import sys
import time

PATH_TO_REPO = Path(os.path.realpath(__file__)).parent.parent
if str(PATH_TO_REPO) not in sys.path:
    sys.path.insert(1, str(PATH_TO_REPO))

PATH_TO_PYTHON_CODE = PATH_TO_REPO.parent / "mas-infrastructure" / "src" / "python"
if str(PATH_TO_PYTHON_CODE) not in sys.path:
    sys.path.insert(1, str(PATH_TO_PYTHON_CODE))

PATH_TO_CAPNP_SCHEMAS = PATH_TO_REPO.parent / "mas-infrastructure" / "capnproto_schemas"
abs_imports = [str(PATH_TO_CAPNP_SCHEMAS)]

import common.common as common

common_capnp = capnp.load(str(PATH_TO_CAPNP_SCHEMAS / "common.capnp"), imports=abs_imports)

#------------------------------------------------------------------------------

config = {
    "in_sr": None, 
    "out_sr": None, 
    "id_attr": "id",
    "to_attr": "out_path",
    "bin_size": "1000",
    "dir_template": "out/{}",
}
common.update_config(config, sys.argv, print_config=True, allow_new_keys=False)

conman = common.ConnectionManager()
inp = conman.try_connect(config["in_sr"], cast_as=common_capnp.Channel.Reader, retry_secs=1)
outp = conman.try_connect(config["out_sr"], cast_as=common_capnp.Channel.Writer, retry_secs=1)
bin_size = int(config["bin_size"])
count = defaultdict(lambda: 0)

try:
    if inp and outp:
        while True:
            in_msg = inp.read().wait()
            # check for end of data from in port
            if in_msg.which() == "done":
                break
            
            in_ip = in_msg.value.as_struct(common_capnp.IP)
            id = common.get_fbp_attr(in_ip, config["id_attr"]).as_text()
            if id:
                id_ = int(id.split("_")[0])
                count[id_] += 1
                count_bin = int(len(count) / bin_size)
                dir = config["dir_template"].format("ids_" + str(count_bin*bin_size) + "-" + str((count_bin+1)*bin_size)) 

                out_ip = common_capnp.IP.new_message(content=in_ip.content)
                common.copy_fbp_attr(in_ip, out_ip, config["to_attr"], dir)
                outp.write(value=out_ip).wait()

        # close out port
        outp.write(done=None).wait()

except Exception as e:
    print("create_out_path.py ex:", e)

print("create_out_path.py: exiting run")
