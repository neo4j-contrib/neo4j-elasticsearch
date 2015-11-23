package org.neo4j.elasticsearch;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Get;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.DeleteIndex;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.Map;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class ElasticSearchEventHandlerTest {

    public static final String INDEX = "test-index";
    public static final String LABEL = "Label";
    private ElasticSearchEventHandler handler;
    private GraphDatabaseService db;
    private JestClient client;

    @Before
    public void setUp() throws Exception {
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig
                .Builder("http://localhost:9200")
                .multiThreaded(true)
                .build());
        client = factory.getObject();
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();

        handler = new ElasticSearchEventHandler(client, ElasticSearchIndexSpecParser.parseIndexSpec(INDEX + ":" + LABEL + "(foo)"), db);
        // don't use async Jest for testing
        handler.setUseAsyncJest(false);
        db.registerTransactionEventHandler(handler);
        
       // create index
       client.execute(new CreateIndex.Builder(INDEX).build());
    }

    @After
    public void tearDown() throws Exception {
        client.execute(new DeleteIndex.Builder(INDEX).build());
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

        JestResult response = client.execute(new Get.Builder(INDEX, id).build());

        assertEquals(true,response.isSucceeded());
        assertEquals(INDEX,response.getValue("_index"));
        assertEquals(id,response.getValue("_id"));
        assertEquals(LABEL,response.getValue("_type"));

        Map source = response.getSourceAsObject(Map.class);
        assertEquals(asList(LABEL), source.get("labels"));
        assertEquals(id, source.get("id"));
        assertEquals("bar", source.get("foo"));
    }

    @Test
    public void testDelete() throws Exception
    {
        Transaction tx = db.beginTx();
        org.neo4j.graphdb.Node node = db.createNode(DynamicLabel.label(LABEL));
        String id = String.valueOf(node.getId());
        node.setProperty("foo","bar");
        tx.success();tx.close();

        JestResult response = client.execute(new Get.Builder(INDEX, id).build());
        assertEquals(true,response.isSucceeded());
        assertEquals(INDEX,response.getValue("_index"));
        assertEquals(id,response.getValue("_id"));
        assertEquals(LABEL,response.getValue("_type"));

        tx = db.beginTx();
        node = db.getNodeById(Integer.parseInt(id));
        assertEquals("bar", node.getProperty("foo")); // check that we get the node that we just added
        node.delete();
        tx.success();tx.close();

        response = client.execute(new Get.Builder(INDEX, id).type(LABEL).build());
        System.out.println(response.getJsonString());
        assertEquals(false, response.getValue("found"));
    }

    @Test
    public void testUpdate() throws Exception {

        Transaction tx = db.beginTx();
        org.neo4j.graphdb.Node node = db.createNode(DynamicLabel.label(LABEL));
        String id = String.valueOf(node.getId());
        node.setProperty("foo","bar");
        tx.success();tx.close();

        JestResult response = client.execute(new Get.Builder(INDEX, id).build());
        assertEquals(true,response.isSucceeded());
        assertEquals(INDEX,response.getValue("_index"));
        assertEquals(id,response.getValue("_id"));
        assertEquals(LABEL,response.getValue("_type"));
        assertEquals("bar", response.getSourceAsObject(Map.class).get("foo"));

        tx = db.beginTx();
        node = db.getNodeById(Integer.parseInt(id));
        node.setProperty("foo", "quux");
        tx.success(); tx.close();

        response = client.execute(new Get.Builder(INDEX, id).type(LABEL).build());
        assertEquals(true,response.isSucceeded());
        assertEquals(true, response.getValue("found"));
        assertEquals("quux", response.getSourceAsObject(Map.class).get("foo"));
    }


}
