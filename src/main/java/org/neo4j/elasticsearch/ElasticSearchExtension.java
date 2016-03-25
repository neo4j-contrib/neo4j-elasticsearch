package org.neo4j.elasticsearch;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.lifecycle.Lifecycle;

import java.util.Map;
import java.util.logging.Logger;
import java.text.ParseException;

/**
 * @author mh
 * @since 25.04.15
 */
public class ElasticSearchExtension implements Lifecycle {
    private final GraphDatabaseService gds;
    private final static Logger logger = Logger.getLogger(ElasticSearchExtension.class.getName());
    private final String hostName;
    private boolean enabled = true;
    private ElasticSearchEventHandler handler;
    private JestClient client;
    private Map indexSpec;

    public ElasticSearchExtension(GraphDatabaseService gds, String hostName, String indexSpec) {
        Map iSpec;
        try {
            iSpec = ElasticSearchIndexSpecParser.parseIndexSpec(indexSpec);
            if (iSpec.size() == 0) {
                logger.severe("ElasticSearch Integration: syntax error in index_spec");
                enabled = false;
            }
            this.indexSpec = iSpec;
        } catch (ParseException e) {
            logger.severe("ElasticSearch Integration: Can't define index twice");
            enabled = false;
        }
        logger.info("Elasticsearch Integration: Running " + hostName + " - " + indexSpec);
        this.gds = gds;
        this.hostName = hostName;
    }

    @Override
    public void init() throws Throwable {
        if (!enabled) return;

        client = getJestClient(hostName);
        handler = new ElasticSearchEventHandler(client,indexSpec,gds);
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
        if (!enabled) return;
        gds.unregisterTransactionEventHandler(handler);
        client.shutdownClient();
        logger.info("Disconnected from ElasticSearch");
    }

    private JestClient getJestClient(final String hostName) throws Throwable {
      JestClientFactory factory = new JestClientFactory();
      factory.setHttpClientConfig(JestDefaultHttpConfigFactory.getConfigFor(hostName));
      return factory.getObject();
    }
}
