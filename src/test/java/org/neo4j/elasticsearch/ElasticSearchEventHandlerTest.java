package org.neo4j.elasticsearch;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Get;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.util.TestLogger;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.Map;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class ElasticSearchEventHandlerTest {

    public static final String LABEL = "Label";
    private ElasticSearchEventHandler handler;
    private GraphDatabaseService db;
    private JestClient client;
    private TestLogger logger;

    @Before
    public void setUp() throws Exception {
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig
                .Builder("http://localhost:9200")
                .multiThreaded(true)
                .build());
        client = factory.getObject();
        logger = new TestLogger();
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        handler = new ElasticSearchEventHandler(client, "test-index", LABEL, logger, db);
        db.registerTransactionEventHandler(handler);
    }

    @After
    public void tearDown() throws Exception {
        client.shutdownClient();
        db.unregisterTransactionEventHandler(handler);
        db.shutdown();
    }

    @Test
    public void testAfterCommit() throws Exception {
        Transaction tx = db.beginTx();
        org.neo4j.graphdb.Node node = db.createNode(DynamicLabel.label(LABEL));
        String id = String.valueOf(node.getId());
        node.setProperty("foo","bar");
        tx.success();tx.close();

        JestResult response = client.execute(new Get.Builder("test-index", id).build());

        assertEquals(true,response.isSucceeded());
        assertEquals("test-index",response.getValue("_index"));
        assertEquals(id,response.getValue("_id"));
        assertEquals(LABEL,response.getValue("_type"));


        Map source = response.getSourceAsObject(Map.class);
        assertEquals(asList(LABEL), source.get("labels"));
        assertEquals(id, source.get("id"));
        assertEquals("bar", source.get("foo"));
    }
}
