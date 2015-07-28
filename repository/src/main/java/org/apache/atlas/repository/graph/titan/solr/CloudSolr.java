package org.apache.atlas.repository.graph.titan.solr;

import org.apache.atlas.repository.Constants;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ZkConfigManager;
import org.apache.solr.common.util.NamedList;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class CloudSolr  {

    private static final Logger LOG = LoggerFactory.getLogger(CloudSolr.class);
//  private static final String CONFIGSET_NAME = "conf";

    private String zkAddress;
    private CloudSolrClient client;

    public CloudSolr(String solrZKAddress) {
        client = new CloudSolrClient(solrZKAddress);
        zkAddress = solrZKAddress;
        client.connect();
    }

    public void close() throws IOException {
        client.close();
    }

    public void uploadConfig(String configSetName) {
        Path path = FileSystems.getDefault().getPath("...", "...");
        try {
            //TODO - check if conf already exists ??
            final ZkConfigManager zkConfigManager = new ZkConfigManager(client.getZkStateReader().getZkClient());
            zkConfigManager.uploadConfigDir(path, configSetName);
//            ZkController.uploadConfigDir(server.getZkStateReader().getZkClient(), confDir, configSetName);
        } catch (IOException e) {
//            throw new SolrException.ErrorCode("Unable to upload configuration" , e);
            e.printStackTrace();
        }
    }

    public void addCollections(String configSetName) throws IOException, SolrServerException {

        uploadConfig(configSetName);

        List<String> collectionsToAdd = new ArrayList<String>() {{
            add(Constants.VERTEX_INDEX);
            add(Constants.EDGE_INDEX);
            add(Constants.FULLTEXT_INDEX);
        }};

        for (String collectionName : collectionsToAdd) {
//            final CollectionAdminResponse collection = CollectionAdminRequest.createCollection(collectionName, 1, configSetName, server);
            final CollectionAdminRequest.Create adminRequest = new CollectionAdminRequest.Create();
            adminRequest.setConfigName(configSetName);
            adminRequest.setNumShards(1);
            adminRequest.setReplicationFactor(1);
            final CollectionAdminResponse adminResponse = adminRequest.process(client, collectionName);

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
