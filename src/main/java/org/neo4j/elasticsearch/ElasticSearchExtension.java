package org.neo4j.elasticsearch;

import org.elasticsearch.client.Client;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.Lifecycle;

import static org.elasticsearch.node.NodeBuilder.*;

/**
 * @author mh
 * @since 25.04.15
 */
public class ElasticSearchExtension implements Lifecycle {
    private final GraphDatabaseService gds;
    private final StringLogger logger;
    private final String clusterName;
    private org.elasticsearch.node.Node node;
    private ElasticSearchEventHandler handler;
    private String indexName; // todo configurable, mirror neo4j indexes??
    private String nodeQuery;
    // todo configurable
    private String label;
    private boolean clientOnly = true;

    public ElasticSearchExtension(GraphDatabaseService gds, StringLogger logger, String clusterName, String nodeSelection, String indexName) {
        if (indexName == null || nodeSelection == null)
            throw new IllegalArgumentException("Index-Name and Node Selection (Label) must be set");
        this.gds = gds;
        this.logger = logger;
        this.clusterName = clusterName;
        this.label = nodeSelection;
        this.indexName = indexName;
    }

    @Override
    public void init() throws Throwable {
        node = nodeBuilder().clusterName(clusterName).client(clientOnly).node();
        Client client = node.client();
        handler = new ElasticSearchEventHandler(client, indexName,label);
        gds.registerTransactionEventHandler(handler);
        logger.info("Connecting to ElasticSearch");
    }

    @Override
    public void start() throws Throwable {
    }

    @Override
    public void stop() throws Throwable {

    }

    @Override
    public void shutdown() throws Throwable {
        gds.unregisterTransactionEventHandler(handler);
        node.close();
        logger.info("Disconnected from ElasticSearch");
    }

}
