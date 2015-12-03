package org.apache.atlas.repository.graph;

import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.util.TitanCleanup;
import com.thinkaurelius.titan.diskstorage.Backend;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.commons.configuration.Configuration;
import org.junit.Before;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

@Test
public class TitanGraphProviderTest {

   private TitanGraph graph;
   private Configuration configuration;

   @BeforeTest
   public void setUp() throws AtlasException {
       //First get Instance
       graph = TitanGraphProvider.getGraphInstance();
       configuration = ApplicationProperties.getSubsetConfiguration(ApplicationProperties.get(), TitanGraphProvider.GRAPH_PREFIX);
   }

    @AfterClass
    public void tearDown() throws Exception {
        try {
            graph.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            TitanCleanup.clear(graph);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

   @Test
   public void testValidate() throws AtlasException {
       try {
           TitanGraphProvider.validateIndexBackend(configuration);
       } catch(Exception e){
           Assert.fail("Unexpected exception ", e);
       }

       //Change backend
       configuration.setProperty(TitanGraphProvider.INDEX_BACKEND_CONF, TitanGraphProvider.INDEX_BACKEND_LUCENE);
       try {
           TitanGraphProvider.validateIndexBackend(configuration);
           Assert.fail("Expected exception");
       } catch(Exception e){
           Assert.assertEquals(e.getMessage(), "Configured Index Backend lucene differs from earlier configured Index Backend elasticsearch. Aborting!");
       }
   }
}
