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
import time
import sys

PATH_TO_REPO = Path(os.path.realpath(__file__)).parent.parent
if str(PATH_TO_REPO) not in sys.path:
    sys.path.insert(1, str(PATH_TO_REPO))

PATH_TO_PYTHON_CODE = PATH_TO_REPO / "../mas-infrastructure/src/python"
if str(PATH_TO_PYTHON_CODE) not in sys.path:
    sys.path.insert(1, str(PATH_TO_PYTHON_CODE))

import common.common as common

PATH_TO_CAPNP_SCHEMAS = (PATH_TO_REPO / "../mas-infrastructure/capnproto_schemas").resolve()
abs_imports = [str(PATH_TO_CAPNP_SCHEMAS)]
common_capnp = capnp.load(str(PATH_TO_CAPNP_SCHEMAS / "common.capnp"), imports=abs_imports) 
x_capnp = capnp.load("bgr_flow_components/x.capnp", imports=abs_imports) 

#------------------------------------------------------------------------------

config = {
    "out_sr": None # utm_coord + id attr
}
common.update_config(config, sys.argv, print_config=True, allow_new_keys=False)

conman = common.ConnectionManager()
outp = conman.try_connect(config["out_sr"], cast_as=common_capnp.Channel.Writer, retry_secs=1)

class X(x_capnp.X.Server):
    def m(self, i, **kwargs):
        return "hello " + str(i)

x = X() #x_capnp.X._new_server(X())

try:
    if outp:
        while True:
            outp.write(value=x).wait()
            #time.sleep(2)

        outp.write(done=None).wait()

except Exception as e:
    print("a.py ex:", e)

print("a.py: exiting run")

