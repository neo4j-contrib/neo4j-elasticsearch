package org.neo4j.elasticsearch;

import java.util.List;
import java.util.Map;

public class ElasticSearchIndexSettings {
    private Map<String, List<ElasticSearchIndexSpec>> indexSpec;
    private boolean includeIDField;
    private boolean includeLabelsField;

    public ElasticSearchIndexSettings(Map indexSpec, boolean includeIDField, boolean includeLabelsField) {
    	this.indexSpec = indexSpec;
    	this.includeIDField = includeIDField;
    	this.includeLabelsField = includeLabelsField;
    }
    
    public Map<String, List<ElasticSearchIndexSpec>> getIndexSpec() {
    	return indexSpec;
    }
    public boolean getIncludeIDField() {
    	return includeIDField;
    }
    public void setIncludeIDField(boolean value) {
        includeIDField = value;
    }
    public boolean getIncludeLabelsField() {
    	return includeLabelsField;
    }
    public void setIncludeLabelsField(boolean value) {
        includeLabelsField = value;
    }
}
