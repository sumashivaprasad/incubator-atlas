package org.apache.atlas.repository.graph.titan.solr;


import com.google.common.base.Joiner;
import com.google.inject.Provides;
import org.apache.atlas.repository.Constants;
import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

@Singleton
public class EmbeddedSolr {

    private static final Logger LOG = LoggerFactory.getLogger(EmbeddedSolr.class);

    private static final String SOLR_HOME = "solr.solr.home";
    private static final String CORE_NAME = "collection1";
//    private static final String CONFIGSET_NAME = "collection1";
    private String solrHome;

    private EmbeddedSolrServer server;

    private CoreContainer container;

    private static EmbeddedSolr INSTANCE;

    private EmbeddedSolr() {
        String userDir = System.getProperty("user.dir");
        String defaultSolrHome = Joiner.on(File.separator).join(userDir, "target", "classes", "titan-solr", "solr");
        solrHome = System.getProperty(SOLR_HOME, defaultSolrHome);
        container = new CoreContainer(solrHome);
//        CollectionsHandler handler = container.getCollectionsHandler();
        container.load();
    }

    public void start() throws Exception {
        // Note that the following property could be set through JVM level arguments too
//      System.getProperty("solr.solr.home", "/home/shalinsmangar/work/oss/branch-1.3/example/solr");
//      FileUtilities.deleteDirectory("testdata/solr/collection1/data");
//        System.getProperty(SOLR_HOME, "/titan-solr/solr");
        server = new EmbeddedSolrServer(container, CORE_NAME);
        init();
    }

    public static EmbeddedSolr get() {
        if(INSTANCE == null) {
            synchronized (EmbeddedSolr.class) {
                if(INSTANCE == null) {
                    INSTANCE = new EmbeddedSolr();
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

    public void init() throws Exception {
//        addCores();
//        uploadConfigDirToZk(CORE_NAME, Joiner.on(File.separator).join(solrHome, CORE_NAME), CONFIGSET_NAME);
//        addCollections();
//        server.commit();
    }

//    private void addCores() throws IOException, SolrServerException {
//        CoreAdminRequest.createCore(CORE_NAME, Joiner.on(File.separator).join(solrHome, CORE_NAME), server);
//    }

//    private void addCollections() throws IOException, SolrServerException {
//        List<String> collectionsToAdd = new ArrayList<String>() {{
//            add(Constants.VERTEX_INDEX);
//            add(Constants.EDGE_INDEX);
//            add(Constants.FULLTEXT_INDEX);
//        }};
//
//        for (String collectionName : collectionsToAdd) {
//            final CollectionAdminResponse collection = CollectionAdminRequest.createCollection(collectionName, 1, CONFIGSET_NAME, server);
//            if (!collection.isSuccess()) {
//                final NamedList<String> errorMessages = collection.getErrorMessages();
//                Iterator errorMsgIter = errorMessages.iterator();
//                while (errorMsgIter.hasNext()) {
//                    LOG.error("Failed to create collection " + collectionName + errorMsgIter.next());
//                }
//                throw new RuntimeException("Failed to initialize solr");
//            }
//        }
//    }

    protected void uploadConfigDirToZk(String coreName, String collectionConfigDir, String configSetname) throws Exception {
        ZkController zkController = container.getZkController();
        zkController.uploadConfigDir(new File(collectionConfigDir), configSetname);
        zkController.linkConfSet(zkController.getZkClient(), coreName, configSetname);
        zkController.bootstrapConf(zkController.getZkClient(), container, solrHome);
    }

    public static void main(String[] args) {
        String solrZkAddress = System.getProperty("solr.zk.address", "localhost:2182");
        System.setProperty("zkRun", solrZkAddress);

//      System.setProperty("bootstrap_confdir", "true");
        System.setProperty("bootstrap_conf", "true");
        System.setProperty(SOLR_HOME, "/Users/sshivaprasad/workspace/incubator-atlas/repository/src/main/resources/titan-solr/solr");
        try {
            EmbeddedSolr.get().start();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SolrServerException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }

        EmbeddedSolr.get().stop();
    }

    public String getAddress() {
        return container.getZkController().getZkServerAddress();
    }
}
