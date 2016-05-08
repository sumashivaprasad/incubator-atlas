/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.hive.bridge;

import org.apache.atlas.hive.rewrite.HiveASTRewriter;
import org.apache.atlas.hive.rewrite.HiveEventContext;
import org.apache.atlas.hive.rewrite.RewriteException;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class HivePartitionRewriterTest {

    private HiveConf conf;

    @BeforeClass
    public void setup() {
        conf = new HiveConf();
        conf.addResource("/hive-site.xml");
        SessionState ss = new SessionState(conf, "testuser");
        SessionState.start(ss);
        conf.set("hive.lock.manager", "org.apache.hadoop.hive.ql.lockmgr.EmbeddedLockManager");
    }

    @Test
    public void testPartitionRewrite() {
        HiveEventContext ctx = new HiveEventContext();
        ctx.setIsPartitionBasedQuery(true);
        ctx.setQueryStr("insert into table testTable partition(dt='2014-01-01') select * from test1 where dt = '2014-01-01'");

        try {
            HiveASTRewriter queryRewriter  = new HiveASTRewriter(ctx, conf);
            String result = queryRewriter.rewrite(ctx.getQueryStr());
            System.out.println(" translated sql : " + result);
        } catch (RewriteException e) {
            e.printStackTrace();
        }
    }
}
