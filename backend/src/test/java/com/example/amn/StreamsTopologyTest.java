package com.example.amn;

import static org.apache.kafka.common.serialization.Serdes.String;
import static org.junit.jupiter.api.Assertions.assertEquals;
import java.math.BigInteger;
import java.nio.file.Files;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.junit.jupiter.api.Test;

class StreamsTopologyTest {
  @Test void materializesAggregateAndSuppressesDuplicateIds() {
    StreamsBuilder builder = new StreamsBuilder();
    var output = new java.util.ArrayList<String>();
    var report = new ReportService("/tmp/streams-topology-test.csv") {
      @Override public synchronized void replaceAggregate(String client, String product, BigInteger total) {
        output.add(client + "/" + product + "=" + total);
      }
    };
    new StreamsTopology().transactionAggregates(builder, report, "transactions");
    Properties properties = new Properties();
    properties.put("application.id", "topology-test");
    properties.put("bootstrap.servers", "dummy:1234");
    try (TopologyTestDriver driver = new TopologyTestDriver(builder.build(), properties)) {
      TestInputTopic<String, Transaction> input = driver.createInputTopic("transactions", String().serializer(), new org.springframework.kafka.support.serializer.JsonSerde<>(Transaction.class).serializer());
      input.pipeInput("a", new Transaction("1", "CL|1", "P|1", BigInteger.valueOf(5), 0));
      input.pipeInput("duplicate", new Transaction("1", "CL|1", "P|1", BigInteger.valueOf(5), 0));
      input.pipeInput("b", new Transaction("2", "CL|1", "P|1", BigInteger.valueOf(-2), 1));
      assertEquals(BigInteger.valueOf(3),
          driver.getKeyValueStore(StreamsTopology.STORE_NAME).get("CL|1\0P|1"));
    }
    assertEquals(java.util.List.of("CL|1/P|1=5", "CL|1/P|1=3"), output);
  }

  @Test void hydratesReportFromRestoredStoreWithoutNewEvents() throws Exception {
    var output = Files.createTempFile("restored-report", ".csv");
    var report = new ReportService(output.toString());
    report.replaceAggregate("stale", "row", BigInteger.ONE);
    ReadOnlyKeyValueStore<String, BigInteger> store = new ReadOnlyKeyValueStore<>() {
      @Override public BigInteger get(String key) { return null; }
      @Override public KeyValueIterator<String, BigInteger> range(String from, String to) { return all(); }
      @Override public KeyValueIterator<String, BigInteger> reverseRange(String from, String to) { return all(); }
      @Override public KeyValueIterator<String, BigInteger> all() { return iterator(); }
      @Override public KeyValueIterator<String, BigInteger> reverseAll() { return iterator(); }
      @Override public long approximateNumEntries() { return 1; }
      private KeyValueIterator<String, BigInteger> iterator() {
        var iterator = Map.of("CL|9\0P|9", BigInteger.valueOf(42)).entrySet().iterator();
        return new KeyValueIterator<>() {
          @Override public void close() {}
          @Override public String peekNextKey() { return iterator.next().getKey(); }
          @Override public boolean hasNext() { return iterator.hasNext(); }
          @Override public KeyValue<String, BigInteger> next() {
            var entry = iterator.next(); return KeyValue.pair(entry.getKey(), entry.getValue());
          }
        };
      }
    };
    StreamsTopology.hydrateReport(report, store);
    assertEquals("Client_Information,Product_Information,Total_Transaction_Amount\nCL|9,P|9,42\n",
        Files.readString(output));
  }
}
