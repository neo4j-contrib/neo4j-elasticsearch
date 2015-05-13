package org.neo4j.elasticsearch;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.*;

import org.neo4j.graphdb.Label;

@Ignore
public class ElasticSearchIndexSpecParserTest {
  
    @Test
    public void testParseIndexSpec() throws ParseException {
        Map rv = ElasticSearchIndexSpecParser.parseIndexSpec("index_name:Label(foo,bar,quux),other_index_name:OtherLabel(baz,quuxor)");
        List<String> r = new ArrayList<String>();
        for (Object l: rv.keySet()) {
            r.add(((Label) l).name());
        }
        assertEquals(2, rv.size());
        assertArrayEquals(new String[] { "Label", "OtherLabel" }, r.toArray());
    }
    
    @Test
    public void testIndexSpecBadSyntax() throws ParseException {
        Map rv = ElasticSearchIndexSpecParser.parseIndexSpec("index_name:Label(foo,bar");
        assertEquals(0, rv.size());
        rv = ElasticSearchIndexSpecParser.parseIndexSpec("index_name:Label");
        assertEquals(0, rv.size());
        rv = ElasticSearchIndexSpecParser.parseIndexSpec("Label");
        assertEquals(0, rv.size());
    }
    
    @Test(expected=ParseException.class)
    public void testIndexSpecBadSyntaxDuplicateIndex() throws ParseException {
    	Map rv = ElasticSearchIndexSpecParser.parseIndexSpec("index_name:Label(foo,bar),index_name:Label(quux)");
    }

    
}
