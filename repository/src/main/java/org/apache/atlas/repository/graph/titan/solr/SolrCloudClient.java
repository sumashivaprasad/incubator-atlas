package org.apache.atlas.repository.graph.titan.solr;

import com.google.common.base.Joiner;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.repository.Constants;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.util.NamedList;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class SolrCloudClient {

    private static final Logger LOG = LoggerFactory.getLogger(SolrCloudClient.class);
    private String zkAddress;
    private String solrHome;
    private CloudSolrServer client;
    private static final String SOLR_HOME = "solr.solr.home";

    public SolrCloudClient(String solrZKAddress) {
        client = new CloudSolrServer(solrZKAddress);
        zkAddress = solrZKAddress;

    }

    public void connect() {
        client.connect();
    }

    public void close() throws IOException {
        client.shutdown();
    }

    public void uploadConfig(String configSetName) {
        String userDir = System.getProperty("user.dir");
        String defaultSolrHome = Joiner.on(File.separator).join(userDir, "target", "classes", "titan", "solr");
        solrHome = System.getProperty(SOLR_HOME, defaultSolrHome);
        File vertexIndexConf = new File(solrHome, "vertex_index");
        try {
            ZkController.uploadConfigDir(client.getZkStateReader().getZkClient(), vertexIndexConf, configSetName);
        } catch (IOException e) {
            throw new RuntimeException("Could not upload configuration to zookeeper ", e);
        } catch (InterruptedException e) {
            throw new RuntimeException("Could not upload configuration to zookeeper ", e);
        } catch (KeeperException e) {
            throw new RuntimeException("Could not upload configuration to zookeeper ", e);
        }
    }

    public void addCollections(String configSetName) throws IOException, SolrServerException, AtlasException {
        uploadConfig(configSetName);

        List<String> collectionsToAdd = new ArrayList<String>() {{
            add(Constants.VERTEX_INDEX);
            add(Constants.EDGE_INDEX);
            add(Constants.FULLTEXT_INDEX);
        }};

        for (String collectionName : collectionsToAdd) {
            final CollectionAdminRequest.Create adminRequest = new CollectionAdminRequest.Create();
            adminRequest.setCollectionName(collectionName);
            adminRequest.setConfigName(configSetName);
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
        }
        client.commit();
    }
}
