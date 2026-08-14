package com.example.amn;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
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
  KTable<String, Long> transactionAggregates(StreamsBuilder builder, ReportService report,
      @Value("${amn.topic:transactions}") String topic) {
    JsonSerde<Transaction> transactionSerde = new JsonSerde<>(Transaction.class);
    // ponytail: unbounded; daily file fits. Cap/TTL punctuator if ids survive across days.
    builder.addStateStore(Stores.keyValueStoreBuilder(
        Stores.persistentKeyValueStore(SEEN_IDS_STORE), Serdes.String(), Serdes.Long()));
    KTable<String, Long> aggregates = builder
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
        // ponytail: long; overflow ~9e8 max-qty records. BigInteger if that happens.
        .aggregate(() -> 0L, (key, transaction, total) -> Math.addExact(total, transaction.delta()),
            Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as(STORE_NAME)
                .withKeySerde(Serdes.String()).withValueSerde(totalSerde()));
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
        ReadOnlyKeyValueStore<String, Long> store = factory.getKafkaStreams().store(
            org.apache.kafka.streams.StoreQueryParameters.fromNameAndType(
                STORE_NAME, QueryableStoreTypes.keyValueStore()));
        hydrateReport(report, store);
      }
    });
  }

  static void hydrateReport(ReportService report, ReadOnlyKeyValueStore<String, Long> store) {
    List<ReportService.Aggregate> restored = new ArrayList<>();
    try (var entries = store.all()) {
      while (entries.hasNext()) {
        KeyValue<String, Long> entry = entries.next();
        String[] parts = entry.key.split(KEY_SEPARATOR, -1);
        restored.add(new ReportService.Aggregate(parts[0], parts[1], entry.value));
      }
    }
    report.replaceAll(restored);
  }

  // ponytail: 8-byte Long plus UTF-8 decimal fallback so pre-long changelogs restore.
  static org.apache.kafka.common.serialization.Serde<Long> totalSerde() {
    Serializer<Long> serializer = Serdes.Long().serializer();
    Deserializer<Long> nativeLong = Serdes.Long().deserializer();
    Deserializer<Long> deserializer = (topic, bytes) -> {
      if (bytes == null) return null;
      if (bytes.length == 8) return nativeLong.deserialize(topic, bytes);
      return Long.parseLong(new String(bytes, StandardCharsets.UTF_8));
    };
    return Serdes.serdeFrom(serializer, deserializer);
  }
}
