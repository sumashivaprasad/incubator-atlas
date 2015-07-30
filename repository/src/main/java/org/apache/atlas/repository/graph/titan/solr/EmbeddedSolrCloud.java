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
import org.apache.atlas.AtlasException;
import org.apache.atlas.repository.Constants;
import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.servlet.SolrDispatchFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

@Singleton
public class EmbeddedSolrCloud {

    private static final Logger LOG = LoggerFactory.getLogger(EmbeddedSolrCloud.class);

    private static final String SOLR_HOME = "solr.solr.home";
    private static final String CONFIGSET_NAME = "atlasConf";
    private String solrHome;

    private MiniSolrCloudCluster server;

    private static EmbeddedSolrCloud INSTANCE;

    private EmbeddedSolrCloud() {
        String userDir = System.getProperty("user.dir");
        System.setProperty("bootstrap_conf", "true");
        System.setProperty("numShards", "1");

//        String defaultSolrHome = Joiner.on(File.separator).join(userDir, "target", "classes", "titan", "solr");
        URL url = this.getClass().getResource("/titan/solr/solr.xml");
        String solrXMLPath = url.getFile();
        File solrXmlFile = new File(solrXMLPath);
        LOG.info("Solr.xml path : " + solrXMLPath);
        solrHome = System.getProperty(SOLR_HOME, solrXmlFile.getParentFile().getAbsolutePath());
        LOG.info("Solr home : " + solrHome);
        if(System.getProperty(SOLR_HOME) == null) {
            System.setProperty(SOLR_HOME, solrHome);
        }
    }

    public void start() throws Exception {
      server = new MiniSolrCloudCluster(1, "/solr", new File(solrHome, "solr.xml"), null, null);
//      init();
    }

    public void init() throws Exception {
        uploadConfigDirToZk("vertex_index", Joiner.on(File.separator).join(solrHome, "vertex_index", "conf"));
        uploadConfigDirToZk("edge_index", Joiner.on(File.separator).join(solrHome, "edge_index", "conf"));
        uploadConfigDirToZk("fulltext_index", Joiner.on(File.separator).join(solrHome, "fulltext_index", "conf"));

        addCollections("vertex_index");
        addCollections("edge_index");
        addCollections("fulltext_index");
    }

    public void addCollections(String collectionName) throws IOException, SolrServerException, AtlasException {
//        uploadConfig(configSetName);

        CloudSolrServer client = new CloudSolrServer(getZkAddress());

            final CollectionAdminRequest.Create adminRequest = new CollectionAdminRequest.Create();

            adminRequest.setCollectionName(collectionName);
            adminRequest.setConfigName(collectionName);

            final CollectionAdminResponse adminResponse = adminRequest.process(client);

            if (!adminResponse.isSuccess()) {
                final NamedList<String> errorMessages = adminResponse.getErrorMessages();
                Iterator errorMsgIter = errorMessages.iterator();
                while (errorMsgIter.hasNext()) {
                    LOG.error("Failed to create collection " + collectionName + errorMsgIter.next());
                }
                client.rollback();
                throw new RuntimeException("Failed to initialize solr");
            } else {
                LOG.info("Successfully created collection " + collectionName);
            }
        client.commit();
    }

    private ZkController getZkController() {
        SolrDispatchFilter dispatchFilter =
            (SolrDispatchFilter) server.getJettySolrRunners().get(0).getDispatchFilter().getFilter();
        return dispatchFilter.getCores().getZkController();
    }

    private CoreContainer getCoreContainer() {
        SolrDispatchFilter dispatchFilter =
            (SolrDispatchFilter) server.getJettySolrRunners().get(0).getDispatchFilter().getFilter();
        return dispatchFilter.getCores();
    }

    protected void uploadConfigDirToZk(String coreName, String collectionConfigDir) throws Exception {
        ZkController zkController = getZkController();
        zkController.uploadConfigDir(new File(collectionConfigDir), coreName);
    }

    public static EmbeddedSolrCloud get() {
        if(INSTANCE == null) {
            synchronized (EmbeddedSolrCloud.class) {
                if(INSTANCE == null) {
                    INSTANCE = new EmbeddedSolrCloud();
                }
            }
        }
        return INSTANCE;
    }

    public void stop() {
        try {
            if (server != null) {
                LOG.info("Shutting down embedded solr");
                server.shutdown();
                getCoreContainer().getCore("vertex_index").get
                getCoreContainer().unload("vertex_index", true, true, true);
                getCoreContainer().unload("edge_index", true, true, true);
                getCoreContainer().unload("fulltext_index", true, true, true);
                server = null;
            }
            FileUtils.deleteDirectory(new File(Joiner.on(File.separator).join(solrHome, "zoo_data")));
        } catch (IOException e) {
            LOG.info("Could not stop solr cloud ", e);
        } catch (Exception e) {
            LOG.info("Could not stop solr cloud ", e);
        }
    }

    public static void main(String[] args) {
        try {
            EmbeddedSolrCloud.get().start();
        } catch (Exception e) {
            e.printStackTrace();
        }
//        EmbeddedSolrCloud.get().stop();
    }

    public String getZkAddress() {
        return server.getZkServer().getZkAddress();
    }

    public String getSolrHttpAddress() {
        return server.getJettySolrRunners().get(0).getBaseUrl().getPath();
    }
}
