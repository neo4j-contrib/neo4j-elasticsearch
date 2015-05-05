package org.neo4j.elasticsearch;

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
    private final StringLogger logger;
    private final GraphDatabaseService gds;
    private final Map<Label, List<ElasticSearchIndexSpec>> indexSpecs;
    private Set<Label> indexLabels;

    public ElasticSearchEventHandler(JestClient client, Map<Label, List<ElasticSearchIndexSpec>> indexSpec, StringLogger logger, GraphDatabaseService gds) {
        this.client = client;
        this.indexSpecs = indexSpec;
        this.indexLabels = indexSpec.keySet();
        this.logger = logger;
        this.gds = gds;
    }

    @Override
    public List<BulkableAction> beforeCommit(TransactionData transactionData) throws Exception {
        List<BulkableAction> actions = new ArrayList<>(1000); 
        for (Node node : transactionData.createdNodes()) {
            if (hasLabel(node)) actions.addAll(indexRequests(node));
        }
        for (Node node : transactionData.deletedNodes()) {
            if (hasLabel(node)) actions.addAll(deleteRequests(node));
        }
        for (LabelEntry labelEntry : transactionData.assignedLabels()) {
            if (hasLabel(labelEntry)) actions.addAll(indexRequests(labelEntry.node()));
        }
        for (LabelEntry labelEntry : transactionData.removedLabels()) {
            if (hasLabel(labelEntry)) actions.addAll(deleteRequests(labelEntry.node(), labelEntry.label()));
        }
        for (PropertyEntry<Node> propEntry : transactionData.assignedNodeProperties()) {
            if (hasLabel(propEntry))
                actions.addAll(indexRequests(propEntry.entity()));
        }
        for (PropertyEntry<Node> propEntry : transactionData.removedNodeProperties()) {
            if (hasLabel(propEntry))
                actions.addAll(updateRequests(propEntry.entity()));
        }
        return actions.isEmpty() ? Collections.<BulkableAction>emptyList() : actions;
    }

    @Override
    public void afterCommit(TransactionData transactionData, List<BulkableAction> actions) {
        if (actions.isEmpty()) return;
        try {
            Bulk bulk = new Bulk.Builder()
                    .addAction(actions).build();
            client.executeAsync(bulk, this);
        } catch (Exception e) {
            logger.warn("Error updating ElasticSearch ", e);
        }
    }

    private boolean hasLabel(Node node) {
        for (Label l: node.getLabels()) {
            if (indexLabels.contains(l)) return true;
        }
        return false;
    }

    private boolean hasLabel(LabelEntry labelEntry) {
        return indexLabels.contains(labelEntry.label());
    }

    private boolean hasLabel(PropertyEntry<Node> propEntry) {
        return hasLabel(propEntry.entity());
    }
    
    private List<Index> indexRequests(Node node) {
    	List <Index> reqs = new ArrayList<Index>();

    	for (Label l: node.getLabels()) {
    		if (!indexLabels.contains(l)) continue;

    		for (ElasticSearchIndexSpec spec: indexSpecs.get(l)) {
    			reqs.add(new Index.Builder(nodeToJson(node, spec.getProperties()))
    			.type(l.name())
    			.index(spec.getIndexName())
    			.id(id(node))
    			.build());
    		}
    	}
    	return reqs;
    }
    
    private List<Delete> deleteRequests(Node node) {
    	List<Delete> reqs = new ArrayList<Delete>();

    	for (Label l: node.getLabels()) {
    		if (!indexLabels.contains(l)) continue;
    		for (ElasticSearchIndexSpec spec: indexSpecs.get(l)) {
    			reqs.add(new Delete.Builder(id(node)).index(spec.getIndexName()).build());
    		}
    	}
    	return reqs;
    	
    }
    
    private List<Delete> deleteRequests(Node node, Label label) {
        List<Delete> reqs = new ArrayList<Delete>();

        if (indexLabels.contains(label)) {
            for (ElasticSearchIndexSpec spec: indexSpecs.get(label)) {
                reqs.add(new Delete.Builder(id(node))
                .index(spec.getIndexName())
                .type(label.name())
                .build());
            }
        }
        return reqs;
        
    }
    
    private List<Update> updateRequests(Node node) {
    	List<Update> reqs = new ArrayList<Update>();
    	for (Label l: node.getLabels()) {
    		if (!indexLabels.contains(l)) continue;

    		for (ElasticSearchIndexSpec spec: indexSpecs.get(l)) {
    			reqs.add(new Update.Builder(nodeToJson(node, spec.getProperties()))
    			.type(l.name())
    			.index(spec.getIndexName())
    			.id(id(node))
    			.build());
    		}
    	}
    	return reqs;
    }


    private String id(Node node) {
        return String.valueOf(node.getId());
    }

    private Map nodeToJson(Node node, Set<String> properties) {
        Map<String,Object> json = new LinkedHashMap<>();
        json.put("id", id(node));
        json.put("labels", labels(node));
        for (String prop : properties) {
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
