/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.repository.graph.titan.solr;

import com.google.common.base.Joiner;
import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.core.CoreContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import java.io.File;
import java.io.IOException;

@Singleton
public class EmbeddedSolr {

    private static final Logger LOG = LoggerFactory.getLogger(EmbeddedSolr.class);

    private static final String SOLR_HOME = "solr.solr.home";
    private static final String CORE_NAME = "atlasCore";
    private String solrHome;

    private String solrZKAddress;

    private EmbeddedSolrServer server;

    private CoreContainer container;

    private static EmbeddedSolr INSTANCE;

    private EmbeddedSolr(String solrZKAddress) {
        String userDir = System.getProperty("user.dir");
        this.solrZKAddress = solrZKAddress;

        System.setProperty("zkRun", solrZKAddress);
        System.setProperty("bootstrap_conf", "true");

        String defaultSolrHome = Joiner.on(File.separator).join(userDir, "target", "classes", "titan", "solr");
        solrHome = System.getProperty(SOLR_HOME, defaultSolrHome);
        container = new CoreContainer(solrHome);
        container.load();
    }

    public void start() {
        server = new EmbeddedSolrServer(container, CORE_NAME);
    }

    public static EmbeddedSolr get(String solrZKAddress) {
        if(INSTANCE == null) {
            synchronized (EmbeddedSolr.class) {
                if(INSTANCE == null) {
                    INSTANCE = new EmbeddedSolr(solrZKAddress);
                }
            }
        }
        return INSTANCE;
    }


    public void stop() {
        server.shutdown();
        try {
            FileUtils.deleteDirectory(new File(Joiner.on(File.separator).join(solrHome, "zoo_data")));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        try {
            EmbeddedSolr.get("localhost:2182").start();
        } catch (Exception e) {
            e.printStackTrace();
        }

        EmbeddedSolr.get("localhost:2182").stop();
    }

    public String getZkAddress() {
        return solrZKAddress;
    }
}
