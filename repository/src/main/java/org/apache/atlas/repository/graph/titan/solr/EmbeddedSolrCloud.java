package org.apache.atlas.repository.graph.titan.solr;


import com.google.common.base.Joiner;
import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.servlet.SolrDispatchFilter;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



import javax.inject.Singleton;
import java.io.File;
import java.io.IOException;

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
//        System.setProperty("zkRun", solrZKAddress);
        System.setProperty("bootstrap_conf", "true");

        String defaultSolrHome = Joiner.on(File.separator).join(userDir, "target", "classes", "titan", "solr");
        solrHome = System.getProperty(SOLR_HOME, defaultSolrHome);
    }

    public void start() throws Exception {
        server = new MiniSolrCloudCluster(1, "/solr", new File(solrHome), new File(solrHome, "solr.xml"), null, null);
//        server = new MiniSolrCloudCluster(1, "/solr", new File(solrHome, "solr.xml"), null, null);
//        init();
    }

//    public void init() throws Exception {
//        uploadConfigDirToZk("vertex_index", Joiner.on(File.separator).join(solrHome, "vertex_index"));
//        uploadConfigDirToZk("edge_index", Joiner.on(File.separator).join(solrHome, "edge_index"));
//        uploadConfigDirToZk("fulltext_index", Joiner.on(File.separator).join(solrHome, "fulltext_index"));
//    }
//
//    private ZkController getZkController() {
//        SolrDispatchFilter dispatchFilter =
//            (SolrDispatchFilter) server.getJettySolrRunners().get(0).getDispatchFilter().getFilter();
//        return dispatchFilter.getCores().getZkController();
//    }
//
//    protected void uploadConfigDirToZk(String coreName, String collectionConfigDir) throws Exception {
//        ZkController zkController = getZkController();
//        zkController.uploadConfigDir(new File(collectionConfigDir), coreName);
//    }

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
                server.shutdown();
            }
            FileUtils.deleteDirectory(new File(Joiner.on(File.separator).join(solrHome, "zoo_data")));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        try {
            EmbeddedSolrCloud.get().start();
        } catch (Exception e) {
            e.printStackTrace();
        }
        EmbeddedSolrCloud.get().stop();
    }

    public String getZkAddress() {
        return server.getZkServer().getZkAddress();
    }

    public String getSolrHttpAddress() {
        return server.getJettySolrRunners().get(0).getBaseUrl().getPath();
    }
}
