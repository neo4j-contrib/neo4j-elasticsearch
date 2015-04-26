package org.neo4j.elasticsearch;

import io.searchbox.action.Action;
import io.searchbox.action.BulkableAction;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.client.JestResultHandler;
import io.searchbox.core.Bulk;
import io.searchbox.core.Delete;
import io.searchbox.core.Index;
import io.searchbox.core.Update;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.event.LabelEntry;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.kernel.impl.util.StringLogger;

import java.util.*;


/**
* @author mh
* @since 25.04.15
*/
class ElasticSearchEventHandler implements TransactionEventHandler<List<BulkableAction>>, JestResultHandler<JestResult> {
    private final JestClient client;
    private final String indexName;
    // todo set of indexName + label + property-names (+include, -exclude)
    private final Label label;
    private final String labelName;
    private final StringLogger logger;
    private final GraphDatabaseService gds;

    public ElasticSearchEventHandler(JestClient client, String indexName, String labelName, StringLogger logger, GraphDatabaseService gds) {
        this.client = client;
        this.indexName = indexName;
        this.labelName = labelName;
        this.logger = logger;
        this.gds = gds;
        this.label = DynamicLabel.label(labelName);
    }

    @Override
    public List<BulkableAction> beforeCommit(TransactionData transactionData) throws Exception {
        List<BulkableAction> actions = new ArrayList<>(1000);
        for (Node node : transactionData.createdNodes()) {
            if (hasLabel(node)) actions.add(indexRequest(node));
        }
        for (Node node : transactionData.deletedNodes()) {
            if (hasLabel(node)) actions.add(deleteRequest(node));
        }
        for (LabelEntry labelEntry : transactionData.assignedLabels()) {
            if (hasLabel(labelEntry)) actions.add(indexRequest(labelEntry.node()));
        }
        for (LabelEntry labelEntry : transactionData.removedLabels()) {
            if (hasLabel(labelEntry)) actions.add(deleteRequest(labelEntry.node()));
        }
        for (PropertyEntry<Node> propEntry : transactionData.assignedNodeProperties()) {
            if (hasLabel(propEntry))
                actions.add(indexRequest(propEntry.entity()));
        }
        for (PropertyEntry<Node> propEntry : transactionData.removedNodeProperties()) {
            if (hasLabel(propEntry))
                actions.add(updateRequest(propEntry.entity()));
        }
        return actions.isEmpty() ? Collections.<BulkableAction>emptyList() : actions;
    }

    @Override
    public void afterCommit(TransactionData transactionData, List<BulkableAction> actions) {
        if (actions.isEmpty()) return;
        try {
            Bulk bulk = new Bulk.Builder()
                    .defaultIndex(indexName)
                    .defaultType(typeName()).addAction(actions).build();
            client.executeAsync(bulk, this);
        } catch (Exception e) {
            logger.warn("Error updating ElasticSearch ", e);
        }
    }

    private boolean hasLabel(Node node) {
        return node.hasLabel(label);
    }

    private boolean hasLabel(LabelEntry labelEntry) {
        return labelEntry.label().name().equals(labelName);
    }

    private boolean hasLabel(PropertyEntry<Node> propEntry) {
        return hasLabel(propEntry.entity());
    }


    private String typeName() {
        return labelName;
    }

    private Index indexRequest(Node node) {
        return new Index.Builder(nodeToJson(node)).id(id(node)).build();
    }

    private Delete deleteRequest(Node node) {
        return new Delete.Builder(id(node)).build();
    }

    private Update updateRequest(Node node) {
        return new Update.Builder(nodeToJson(node)).id(id(node)).build();
    }

    private String id(Node node) {
        return String.valueOf(node.getId());
    }

    private Map nodeToJson(Node node) {
        Map<String,Object> json = new LinkedHashMap<>();
        json.put("id", id(node));
        json.put("labels", labels(node));
        for (String prop : node.getPropertyKeys()) {
            Object value = node.getProperty(prop);
            json.put(prop, value);
        }
        return json;
    }

    private String[] labels(Node node) {
        List<String> result=new ArrayList<>();
        for (Label label : node.getLabels()) {
            result.add(label.name());
        }
        return result.toArray(new String[result.size()]);
    }

    @Override
    public void afterRollback(TransactionData transactionData, List<BulkableAction> actions) {

    }

    @Override
    public void completed(JestResult jestResult) {
        if (jestResult.isSucceeded() && jestResult.getErrorMessage() == null) {
            logger.debug("ElasticSearch Update Success");
        } else {
            logger.warn("ElasticSearch Update Failed: " + jestResult.getErrorMessage());
        }
    }

    @Override
    public void failed(Exception e) {
        logger.warn("Problem Updating ElasticSearch ",e);
    }
}
