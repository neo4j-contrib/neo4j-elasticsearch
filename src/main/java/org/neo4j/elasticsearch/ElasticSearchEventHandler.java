package org.neo4j.elasticsearch;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.event.LabelEntry;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventHandler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
* @author mh
* @since 25.04.15
*/
class ElasticSearchEventHandler implements TransactionEventHandler<Object> {
    public static final TimeValue TIMEOUT = TimeValue.timeValueSeconds(5);
    private final Client client;
    private final String indexName;
    // todo set of indexName + label + property-names (+include, -exclude)
    private final Label label;
    private final String labelName;

    public ElasticSearchEventHandler(Client client, String indexName, String labelName) {
        this.client = client;
        this.indexName = indexName;
        this.labelName = labelName;
        this.label = DynamicLabel.label(labelName);
    }

    @Override
    public Object beforeCommit(TransactionData transactionData) throws Exception {
        return null;
    }

    @Override
    public void afterCommit(TransactionData transactionData, Object o) {
        BulkRequestBuilder req = client.prepareBulk();

        for (Node node : transactionData.createdNodes()) {
            if (hasLabel(node)) req.add(indexRequest(node));
        }
        for (Node node : transactionData.deletedNodes()) {
            if (hasLabel(node)) req.add(deleteRequest(node));
        }
        for (LabelEntry labelEntry : transactionData.assignedLabels()) {
            if (hasLabel(labelEntry)) req.add(updateRequest(labelEntry.node()));
        }
        for (LabelEntry labelEntry : transactionData.removedLabels()) {
            if (hasLabel(labelEntry)) req.add(deleteRequest(labelEntry.node()));
        }
        for (PropertyEntry<Node> propEntry : transactionData.assignedNodeProperties()) {
            if (hasLabel(propEntry))
                req.add(updateRequest(propEntry.entity()));
        }
        for (PropertyEntry<Node> propEntry : transactionData.removedNodeProperties()) {
            if (hasLabel(propEntry))
                req.add(updateRequest(propEntry.entity()));
        }
        req.get(TIMEOUT);
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

    private IndexRequest indexRequest(Node node) {
        return new IndexRequest(indexName, typeName(), id(node)).source(nodeToJson(node));
    }

    private DeleteRequest deleteRequest(Node node) {
        return new DeleteRequest(indexName, typeName(), id(node));
    }

    private UpdateRequest updateRequest(Node node) {
        try {
            return new UpdateRequest(indexName, typeName(),id(node)).source(nodeToJson(node));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private String id(Node node) {
        return String.valueOf(node.getId());
    }

    private XContentBuilder nodeToJson(Node node) {
        try {
            XContentBuilder json = jsonBuilder().startObject();
            json.field("id", id(node));
            json.array("nodeSelection", labels(node));
            for (String prop : node.getPropertyKeys()) {
                Object value = node.getProperty(prop);
                json.field(prop, value);
            }
            return json.endObject();
        } catch(IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    private String[] labels(Node node) {
        List<String> result=new ArrayList<>();
        for (Label label : node.getLabels()) {
            result.add(label.name());
        }
        return result.toArray(new String[result.size()]);
    }

    @Override
    public void afterRollback(TransactionData transactionData, Object o) {

    }
}
