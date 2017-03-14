package org.neo4j.elasticsearch;

import io.searchbox.client.config.HttpClientConfig;

import java.util.*;
import java.util.concurrent.TimeUnit;

import org.junit.*;
import static org.junit.Assert.*;

public class JestDefaultHttpConfigFactoryTest {
  private static HttpClientConfig subject;

  @Before
  public void beforeEach() throws Throwable {
    subject = JestDefaultHttpConfigFactory.getConfigFor("http://localhost:9200", true);
  }

  @Test
  public void itHasTheCorrectHostName() {
    Set<String> expected = new HashSet<String>(Arrays.asList("http://localhost:9200"));
    assertEquals(expected, subject.getServerList());
  }

  @Test
  public void itIsMultiThreaded() {
    assertTrue(subject.isMultiThreaded());
  }

  @Test
  public void itEnablesDiscovery() {
    assertTrue(subject.isDiscoveryEnabled());
  }

  @Test
  public void itDiscoversEveryOne() {
    final Long one = 1L;
    assertEquals(one, subject.getDiscoveryFrequency());
  }

  @Test
  public void itUsesTheMinuteAsTheDiscoveryUnit() {
    assertEquals(TimeUnit.MINUTES, subject.getDiscoveryFrequencyTimeUnit());
  }

  @Test
  public void itDefaultsToHttp() {
    assertEquals("http://", subject.getDefaultSchemeForDiscoveredNodes());
  }

  @Test
  public void itCanSSL() throws Throwable {
    subject = JestDefaultHttpConfigFactory.getConfigFor("https://localhost:9200", true);

    assertEquals("https://", subject.getDefaultSchemeForDiscoveredNodes());
  }
}
