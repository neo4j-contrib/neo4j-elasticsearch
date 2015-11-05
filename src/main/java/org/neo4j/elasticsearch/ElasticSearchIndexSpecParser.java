package org.neo4j.elasticsearch;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Label;

public class ElasticSearchIndexSpecParser {
    
    private final static Pattern INDEX_SPEC_RE = Pattern.compile("(?<indexname>[a-z][a-z_-]+):(?<label>[A-Za-z0-9]+)\\((?<props>[^\\)]+)\\)");
    private final static Pattern PROPS_SPEC_RE = Pattern.compile("((?!=,)([A-Za-z0-9_]+))+");
    
    public static Map<Label, List<ElasticSearchIndexSpec>> parseIndexSpec(String spec) throws ParseException {
        if (spec == null) {
            return new LinkedHashMap<Label, List<ElasticSearchIndexSpec>>();
        }
        Map<Label, List<ElasticSearchIndexSpec>> map = new LinkedHashMap<Label, List<ElasticSearchIndexSpec>>();
        Matcher matcher = INDEX_SPEC_RE.matcher(spec);
        while (matcher.find()) {

            Matcher propsMatcher = PROPS_SPEC_RE.matcher(matcher.group("props"));
            Set<String> props = new HashSet<String>();
            while (propsMatcher.find()) {
                props.add(propsMatcher.group());
            }
            
            Label label = DynamicLabel.label(matcher.group("label"));
            
            if (map.containsKey(label)) {
            	throw new ParseException(matcher.group(), 0);
            }
            map.put(label,
            		new ArrayList<ElasticSearchIndexSpec>(Arrays.asList(new ElasticSearchIndexSpec(matcher.group("indexname"), props))));
        }
        
        return map;
    }
    

}
