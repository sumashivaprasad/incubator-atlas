//package org.apache.atlas.repository.graph.titan.solr;
//
//import org.apache.atlas.repository.Constants;
//import org.apache.solr.client.solrj.SolrServerException;
//import org.apache.solr.client.solrj.impl.CloudSolrServer;
//import org.apache.solr.client.solrj.request.CollectionAdminRequest;
//import org.apache.solr.client.solrj.response.CollectionAdminResponse;
//import org.apache.solr.cloud.ZkController;
//import org.apache.solr.common.SolrException;
//import org.apache.solr.common.util.NamedList;
//import org.apache.zookeeper.KeeperException;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.File;
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.Iterator;
//import java.util.List;
//
//public class CloudSolr  {
//
//    private static final Logger LOG = LoggerFactory.getLogger(CloudSolr.class);
////  private static final String CONFIGSET_NAME = "conf";
//
//    private String zkAddress;
//    private CloudSolrServer server;
//
//    public CloudSolr(String solrZKAddress) {
//        server = new CloudSolrServer(solrZKAddress);
//        zkAddress = solrZKAddress;
//    }
//
//    public void connect() {
//        server.connect();
//    }
//
//    public void shutdown() {
//        server.shutdown();
//    }
//
//    public void uploadConfig(String configSetName) {
//        File confDir = new File("");
////        try {
////            //TODO - check if conf already exists ??
//////            ZkController.uploadConfigDir(server.getZkStateReader().getZkClient(), confDir, configSetName);
////        } catch (IOException e) {
//////            throw new SolrException.ErrorCode("Unable to upload configuration" , e);
////            e.printStackTrace();
////        } catch (KeeperException e) {
////            e.printStackTrace();
////        } catch (InterruptedException e) {
////            e.printStackTrace();
////        }
//
//    }
//
//    public void addCollections(String configSetName) throws IOException, SolrServerException {
//
//        uploadConfig(configSetName);
//
//        List<String> collectionsToAdd = new ArrayList<String>() {{
//            add(Constants.VERTEX_INDEX);
//            add(Constants.EDGE_INDEX);
//            add(Constants.FULLTEXT_INDEX);
//        }};
//
//        for (String collectionName : collectionsToAdd) {
//            final CollectionAdminResponse collection = CollectionAdminRequest.Create(collectionName, 1, configSetName, server);
//            if (!collection.isSuccess()) {
//                final NamedList<String> errorMessages = collection.getErrorMessages();
//                Iterator errorMsgIter = errorMessages.iterator();
//                while (errorMsgIter.hasNext()) {
//                    LOG.error("Failed to create collection " + collectionName + errorMsgIter.next());
//                }
//                server.rollback();
//                throw new RuntimeException("Failed to initialize solr");
//            }
//        }
//
//        server.commit();
//    }
//}
