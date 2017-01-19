# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

Apache Atlas (incubating) Overview

Apache Atlas framework is an extensible set of core
foundational governance services – enabling enterprises to effectively and
efficiently meet their compliance requirements within Hadoop and allows
integration with the whole enterprise data ecosystem.

This will provide true visibility in Hadoop by using both a prescriptive
and forensic model, along with technical and operational audit as well as
lineage enriched by business taxonomical metadata.  It also enables any
metadata consumer to work inter-operably without discrete interfaces to
each other -- the metadata store is common.

The metadata veracity is maintained by leveraging Apache Ranger to prevent
non-authorized access paths to data at runtime.
Security is both role based (RBAC) and attribute based (ABAC).


Apache Atlas is an effort undergoing incubation at the Apache
Software Foundation (ASF), sponsored by the Apache Incubator PMC.

For more information about the incubation status of the Apache Atlas
project you can go to the following page:
http://incubator.apache.org/projects/atlas.html

Build Process
=============

1. Get Atlas sources to your local directory, for example with following commands
   $ cd <your-local-directory>
   $ git clone https://github.com/apache/incubator-atlas.git
   $ cd incubator-atlas

   # Checkout the branch or tag you would like to build
   #
   # to checkout a branch
     $ git checkout <branch>

   # to checkout a tag
     $ git checkout tags/<tag>

2. Execute the following commands to build Apache Atlas

   $ export MAVEN_OPTS="-Xms2g -Xmx2g -XX:MaxPermSize=512M"
   $ mvn clean install

   # currently few tests might fail in some environments
   # (timing issue?), the community is reviewing and updating
   # such tests.
   #
   # if you see test failures, please run the following command:
      $ mvn clean -DskipTests install

   $ mvn clean package -Pdist

3. After above build commands successfully complete, you should see the following files

   webapp/target/atlas-webapp-<version>.war
   addons/falcon-bridge/target/falcon-bridge-<version>.jar
   addons/hive-bridge/target/hive-bridge-<version>.jar
   addons/sqoop-bridge/target/sqoop-bridge-<version>.jar
   addons/storm-bridge/target/storm-bridge-<version>.jar
