#!/usr/bin/env python

'''
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
'''
import sys

from mock import patch
import unittest
import logging
import atlas_config as mc
import atlas_start as metadata
import platform

IS_WINDOWS = platform.system() == "Windows"

logger = logging.getLogger()

class TestMetadata(unittest.TestCase):

  @patch.object(mc,"writePid")
  @patch.object(mc, "executeEnvSh")
  @patch.object(mc,"metadataDir")
  @patch.object(mc, "expandWebApp")
  @patch("os.path.exists")
  @patch.object(mc, "java")
  def test_main(self, java_mock, exists_mock, expandWebApp_mock, metadataDir_mock, executeEnvSh_mock, writePid_mock):
    sys.argv = []
    exists_mock.return_value = True
    expandWebApp_mock.return_value = "webapp"
    metadataDir_mock.return_value = "metadata_home"
    metadata.main()
    self.assertTrue(java_mock.called)
    if IS_WINDOWS:
      java_mock.assert_called_with(
        'org.apache.atlas.Main',
        ['-app', 'metadata_home/server/webapp/atlas'],
        'metadata_home/conf:metadata_home/server/webapp/atlas/WEB-INF/classes:metadata_home/server/webapp/atlas/WEB-INF/lib\\*:metadata_home/libext\\*',
        ['-Datlas.log.dir=metadata_home/logs', '-Datlas.log.file=application.log', '-Datlas.home=metadata_home', '-Datlas.conf=metadata_home/conf', '-Xmx1024m'], 'metadata_home/logs')
    else:
      java_mock.assert_called_with(
        'org.apache.atlas.Main',
        ['-app', 'metadata_home/server/webapp/atlas'],
        'metadata_home/conf:metadata_home/server/webapp/atlas/WEB-INF/classes:metadata_home/server/webapp/atlas/WEB-INF/lib/*:metadata_home/libext/*',
        ['-Datlas.log.dir=metadata_home/logs', '-Datlas.log.file=application.log', '-Datlas.home=metadata_home', '-Datlas.conf=metadata_home/conf', '-Xmx1024m'],  'metadata_home/logs')
    pass


if __name__ == "__main__":
  logging.basicConfig(format='%(asctime)s %(message)s', level=logging.DEBUG)
  unittest.main()