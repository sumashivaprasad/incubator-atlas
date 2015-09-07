#!/usr/bin/env python

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import os
import sys

import atlas_config as mc

METADATA_LOG_OPTS="-Datlas.log.dir=%s -Datlas.log.file=solr.log"
METADATA_COMMAND_OPTS="-Dmetadata.home=%s"

def main():

    metadata_home = mc.metadataDir()
    confdir = mc.dirMustExist(mc.confDir(metadata_home))
    mc.executeEnvSh(confdir)
    logdir = mc.dirMustExist(mc.logDir(metadata_home))

    solrHome = os.environ.get("SOLR_HOME", None)
    if solrHome:
        prg = os.path.join(solrHome, "bin", "solr")
    else:
        prg = which("solr")

    if prg is None:
        raise EnvironmentError('The solr binary could not be found in your path or SOLR_HOME : ' + solrHome)

    collections = ['vertex_index', 'edge_index', 'fulltext_index']
    for i in collections:
        commandline = [prg]
        commandline.append("create -c")
        commandline.append(i)
        process = mc.runProcess(commandline, logdir, "solr")
        process.wait()

    print "Solr collection creation succeeded !!!\n"

if __name__ == '__main__':
    try:
        returncode = main()
    except Exception as e:
        print "Exception: %s " % str(e)
        returncode = -1

    sys.exit(returncode)
