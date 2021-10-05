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

import in_place
import os

def update_files(files):
    for file in files:
        if file.endswith(".asc"):
            with in_place.InPlace(os.path.join(root, file)) as fp:
                for i, line in enumerate(fp):
                    if i == 5:
                        line = line.replace("-999", "-9999")
                    fp.write(line) 

if len(sys.argv) > 1:
    for root, dirs, files in os.walk(argv[1]):
        if len(dirs) > 0:
            for dir in dirs:
                for root, dirs, files in os.walk(os.path.join(root, dir)):
                    update_files(files)
        if len(files) > 0:
            update_files(files)
