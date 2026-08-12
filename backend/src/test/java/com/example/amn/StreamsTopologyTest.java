package com.example.amn;

import static org.apache.kafka.common.serialization.Serdes.String;
import static org.junit.jupiter.api.Assertions.assertEquals;
import java.math.BigInteger;
import java.time.Duration;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.Test;

class StreamsTopologyTest {
  @Test void materializesAggregateAndEmitsReportUpdates() {
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
      input.pipeInput("b", new Transaction("2", "CL|1", "P|1", BigInteger.valueOf(-2), 1));
    }
    assertEquals(java.util.List.of("CL|1/P|1=5", "CL|1/P|1=3"), output);
  }
}
