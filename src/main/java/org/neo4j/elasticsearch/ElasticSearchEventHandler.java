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
class ElasticSearchEventHandler implements TransactionEventHandler<Collection<BulkableAction>>, JestResultHandler<JestResult> {
    private final JestClient client;
    private final StringLogger logger;
    private final GraphDatabaseService gds;
    private final Map<String, List<ElasticSearchIndexSpec>> indexSpecs;
    private final Set<String> indexLabels;
    private boolean useAsyncJest = true;

    public ElasticSearchEventHandler(JestClient client, Map<String, List<ElasticSearchIndexSpec>> indexSpec, StringLogger logger, GraphDatabaseService gds) {
        this.client = client;
        this.indexSpecs = indexSpec;
        this.indexLabels = indexSpec.keySet();
        this.logger = logger;
        this.gds = gds;
    }

    @Override
    public Collection<BulkableAction> beforeCommit(TransactionData transactionData) throws Exception {
        //List<BulkableAction> actions = new ArrayList<>(1000);
        Map<IndexId, BulkableAction> actions = new HashMap<>(1000);
        for (Node node : transactionData.createdNodes()) {
            if (hasLabel(node)) actions.putAll(indexRequests(node));
        }
        for (Node node : transactionData.deletedNodes()) {
            if (hasLabel(node)) actions.putAll(deleteRequests(node));
        }
        for (LabelEntry labelEntry : transactionData.assignedLabels()) {
            if (hasLabel(labelEntry)) actions.putAll(indexRequests(labelEntry.node()));
        }
        for (LabelEntry labelEntry : transactionData.removedLabels()) {
            if (hasLabel(labelEntry)) actions.putAll(deleteRequests(labelEntry.node(), labelEntry.label()));
        }
        for (PropertyEntry<Node> propEntry : transactionData.assignedNodeProperties()) {
            if (hasLabel(propEntry))
                actions.putAll(indexRequests(propEntry.entity()));
        }
        for (PropertyEntry<Node> propEntry : transactionData.removedNodeProperties()) {
            if (hasLabel(propEntry))
                actions.putAll(updateRequests(propEntry.entity()));
        }
        return actions.isEmpty() ? Collections.<BulkableAction>emptyList() : actions.values();
    }

    public void setUseAsyncJest(boolean useAsyncJest) {
        this.useAsyncJest = useAsyncJest;
    }

    @Override
    public void afterCommit(TransactionData transactionData, Collection<BulkableAction> actions) {
        if (actions.isEmpty()) return;
        try {
            Bulk bulk = new Bulk.Builder()
                    .addAction(actions).build();
            if (useAsyncJest) {
                client.executeAsync(bulk, this);
            }
            else {
                client.execute(bulk);
            }
        } catch (Exception e) {
            logger.warn("Error updating ElasticSearch ", e);
        }
    }

    private boolean hasLabel(Node node) {
        for (Label l: node.getLabels()) {
            if (indexLabels.contains(l.name())) return true;
        }
        return false;
    }

    private boolean hasLabel(LabelEntry labelEntry) {
        return indexLabels.contains(labelEntry.label().name());
    }

    private boolean hasLabel(PropertyEntry<Node> propEntry) {
        return hasLabel(propEntry.entity());
    }
    
    private Map<IndexId, Index> indexRequests(Node node) {
        HashMap<IndexId, Index> reqs = new HashMap<>();

        for (Label l: node.getLabels()) {
            String labelName = l.name();
            if (!indexLabels.contains(labelName)) continue;

            for (ElasticSearchIndexSpec spec: indexSpecs.get(labelName)) {
                String id = id(node), indexName = spec.getIndexName();
                reqs.put(new IndexId(indexName, id), new Index.Builder(nodeToJson(node, spec.getProperties()))
                .type(labelName)
                .index(indexName)
                .id(id)
                .build());
            }
        }
        return reqs;
    }

    private Map<IndexId, Delete> deleteRequests(Node node) {
        HashMap<IndexId, Delete> reqs = new HashMap<>();

    	for (Label l: node.getLabels()) {
            String labelName = l.name();
            if (!indexLabels.contains(labelName)) continue;
    		for (ElasticSearchIndexSpec spec: indexSpecs.get(labelName)) {
    		    String id = id(node), indexName = spec.getIndexName();
    			reqs.put(new IndexId(indexName, id),
    			         new Delete.Builder(id).index(indexName).build());
    		}
    	}
    	return reqs;
    }
    
    private Map<IndexId, Delete> deleteRequests(Node node, Label label) {
        HashMap<IndexId, Delete> reqs = new HashMap<>();

        String labelName = label.name();
        if (indexLabels.contains(labelName)) {
            for (ElasticSearchIndexSpec spec: indexSpecs.get(labelName)) {
                String id = id(node), indexName = spec.getIndexName();
                reqs.put(new IndexId(indexName, id),
                         new Delete.Builder(id)
                                   .index(indexName)
                                   .type(labelName)
                                   .build());
            }
        }
        return reqs;
        
    }
    
    private Map<IndexId, Update> updateRequests(Node node) {
    	HashMap<IndexId, Update> reqs = new HashMap<>();
    	for (Label l: node.getLabels()) {
            String labelName = l.name();
            if (!indexLabels.contains(labelName)) continue;

    		for (ElasticSearchIndexSpec spec: indexSpecs.get(labelName)) {
    		    String id = id(node), indexName = spec.getIndexName();
    			reqs.put(new IndexId(indexName, id),
    			        new Update.Builder(nodeToJson(node, spec.getProperties()))
                    			  .type(labelName)
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
    public void afterRollback(TransactionData transactionData, Collection<BulkableAction> actions) {

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
    
    private class IndexId {
        final String indexName, id;
        public IndexId(String indexName, String id) {
            this.indexName = indexName;
            this.id = id;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + getOuterType().hashCode();
            result = prime * result + ((id == null) ? 0 : id.hashCode());
            result = prime * result
                    + ((indexName == null) ? 0 : indexName.hashCode());
            return result;
        }


        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (!(obj instanceof IndexId))
                return false;
            IndexId other = (IndexId) obj;
            if (!getOuterType().equals(other.getOuterType()))
                return false;
            if (id == null) {
                if (other.id != null)
                    return false;
            } else if (!id.equals(other.id))
                return false;
            if (indexName == null) {
                if (other.indexName != null)
                    return false;
            } else if (!indexName.equals(other.indexName))
                return false;
            return true;
        }
        
        private ElasticSearchEventHandler getOuterType() {
            return ElasticSearchEventHandler.this;
        }

        @Override
        public String toString() {
            return "IndexId [indexName=" + indexName + ", id=" + id + "]";
        }
        
    }
    
    
    
}
