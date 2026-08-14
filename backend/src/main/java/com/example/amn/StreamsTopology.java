package com.example.amn;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer;
import org.springframework.kafka.support.serializer.JsonSerde;

@Configuration(proxyBeanMethods = false)
@EnableKafkaStreams
public class StreamsTopology {
  static final String STORE_NAME = "transaction-aggregates";
  static final String SEEN_IDS_STORE = "seen-transaction-ids";
  private static final String KEY_SEPARATOR = "\0";

  @Bean
  KTable<String, BigInteger> transactionAggregates(StreamsBuilder builder, ReportService report,
      @Value("${amn.topic:transactions}") String topic) {
    JsonSerde<Transaction> transactionSerde = new JsonSerde<>(Transaction.class);
    // ponytail: unbounded; daily file fits. Cap/TTL punctuator if ids survive across days.
    builder.addStateStore(Stores.keyValueStoreBuilder(
        Stores.persistentKeyValueStore(SEEN_IDS_STORE), Serdes.String(), Serdes.Long()));
    KTable<String, BigInteger> aggregates = builder
        .stream(topic, Consumed.with(Serdes.String(), transactionSerde))
        .process(() -> new Processor<String, Transaction, String, Transaction>() {
          private KeyValueStore<String, Long> seen;
          private ProcessorContext<String, Transaction> context;
          @Override public void init(ProcessorContext<String, Transaction> context) {
            this.context = context;
            seen = context.getStateStore(SEEN_IDS_STORE);
          }
          @Override public void process(Record<String, Transaction> record) {
            Transaction transaction = record.value();
            if (seen.get(transaction.id()) != null) return;
            seen.put(transaction.id(), transaction.sourcePosition());
            context.forward(record);
          }
        }, SEEN_IDS_STORE)
        .selectKey((ignored, transaction) -> transaction.clientInformation() + KEY_SEPARATOR + transaction.productInformation())
        .groupByKey(Grouped.with(Serdes.String(), transactionSerde))
        .aggregate(() -> BigInteger.ZERO, (key, transaction, total) -> total.add(transaction.delta()),
            Materialized.<String, BigInteger, KeyValueStore<Bytes, byte[]>>as(STORE_NAME)
                .withKeySerde(Serdes.String()).withValueSerde(bigIntegerSerde()));
    aggregates.toStream().foreach((key, total) -> {
      String[] parts = key.split(KEY_SEPARATOR, -1);
      report.replaceAggregate(parts[0], parts[1], total);
    });
    return aggregates;
  }

  @Bean
  StreamsBuilderFactoryBeanConfigurer reportHydrationConfigurer(ReportService report) {
    return factory -> factory.setStateListener((newState, oldState) -> {
      if (newState == org.apache.kafka.streams.KafkaStreams.State.RUNNING) {
        ReadOnlyKeyValueStore<String, BigInteger> store = factory.getKafkaStreams().store(
            org.apache.kafka.streams.StoreQueryParameters.fromNameAndType(
                STORE_NAME, QueryableStoreTypes.keyValueStore()));
        hydrateReport(report, store);
      }
    });
  }

  static void hydrateReport(ReportService report, ReadOnlyKeyValueStore<String, BigInteger> store) {
    List<ReportService.Aggregate> restored = new ArrayList<>();
    try (var entries = store.all()) {
      while (entries.hasNext()) {
        KeyValue<String, BigInteger> entry = entries.next();
        String[] parts = entry.key.split(KEY_SEPARATOR, -1);
        restored.add(new ReportService.Aggregate(parts[0], parts[1], entry.value));
      }
    }
    report.replaceAll(restored);
  }

  static org.apache.kafka.common.serialization.Serde<BigInteger> bigIntegerSerde() {
    Serializer<BigInteger> serializer = (topic, value) -> value == null ? null : value.toString().getBytes(StandardCharsets.UTF_8);
    Deserializer<BigInteger> deserializer = (topic, bytes) -> bytes == null ? null : new BigInteger(new String(bytes, StandardCharsets.UTF_8));
    return Serdes.serdeFrom(serializer, deserializer);
  }
}
