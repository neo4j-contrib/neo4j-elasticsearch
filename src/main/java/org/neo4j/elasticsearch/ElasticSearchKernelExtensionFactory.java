package org.neo4j.elasticsearch;


import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.Description;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.helpers.Settings;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.Lifecycle;

import static org.neo4j.helpers.Settings.*;

/**
 * @author mh
 * @since 06.02.13
 */
public class ElasticSearchKernelExtensionFactory extends KernelExtensionFactory<ElasticSearchKernelExtensionFactory.Dependencies> {

    public static final String SERVICE_NAME = "ELASTIC_SEARCH";

    @Description("Settings for the Elastic Search Extension")
    public static abstract class ElasticSearchSettings {
        public static Setting<HostnamePort> clusterAddress = Settings.setting("elasticsearch.address", HOSTNAME_PORT, ":9300");
        public static Setting<String> hostName = setting("elasticsearch.host_name", STRING, (String) null);
        public static Setting<String> nodeSelection = setting("elasticsearch.node_selection", STRING, (String) null);
        public static Setting<String> indexName = setting("elasticsearch.index_name", STRING, (String)null);
        // todo settings for label, property, indexName
    }

    public ElasticSearchKernelExtensionFactory() {
        super(SERVICE_NAME);
    }

    @Override
    public Lifecycle newKernelExtension(Dependencies dependencies) throws Throwable {
        Config config = dependencies.getConfig();
        return new ElasticSearchExtension(dependencies.getGraphDatabaseService(), dependencies.getStringLogger(),
                config.get(ElasticSearchSettings.hostName),
                config.get(ElasticSearchSettings.nodeSelection),
                config.get(ElasticSearchSettings.indexName));
    }

    public interface Dependencies {
        GraphDatabaseService getGraphDatabaseService();

        StringLogger getStringLogger();

        Config getConfig();
    }
}
