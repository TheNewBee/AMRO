package com.example.amn;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class KafkaBrokerIT {
  private static final String APP_ID = "amn-broker-it-" + UUID.randomUUID();
  private static final String TOPIC = "transactions-" + APP_ID;
  private static final String DLQ = TOPIC + ".DLT";
  private static final String RECORD_1 = record315("CL", "0001", "0002", "0003",
      "FU", "XNYM", "ABC", "20200101", "0000000003", "0000000001");
  private static final String RECORD_2 = record315("CL", "0001", "0002", "0003",
      "FU", "XNYM", "ABC", "20200101", "0000000004", "0000000000");
  private static final String RECORD_3 = record315("CL", "0001", "0002", "0003",
      "FU", "XNYM", "XYZ", "20200101", "0000000005", "0000000000");
  private static final String MALFORMED = "x".repeat(176);
  private static final String CSV_AFTER_STARTUP =
      "Client_Information,Product_Information,Total_Transaction_Amount\n"
          + "CL|0001|0002|0003,XNYM|FU|ABC|20200101,2\n";
  private static final String CSV_AFTER_APPEND =
      "Client_Information,Product_Information,Total_Transaction_Amount\n"
          + "CL|0001|0002|0003,XNYM|FU|ABC|20200101,6\n";
  private static final String CSV_FINAL =
      "Client_Information,Product_Information,Total_Transaction_Amount\n"
          + "CL|0001|0002|0003,XNYM|FU|ABC|20200101,6\n"
          + "CL|0001|0002|0003,XNYM|FU|XYZ|20200101,5\n";
  private static final String JSON_AFTER_STARTUP =
      "[{\"clientInformation\":\"CL|0001|0002|0003\",\"productInformation\":\"XNYM|FU|ABC|20200101\",\"totalTransactionAmount\":\"2\"}]";
  private static final String JSON_FINAL =
      "[{\"clientInformation\":\"CL|0001|0002|0003\",\"productInformation\":\"XNYM|FU|ABC|20200101\",\"totalTransactionAmount\":\"6\"},"
          + "{\"clientInformation\":\"CL|0001|0002|0003\",\"productInformation\":\"XNYM|FU|XYZ|20200101\",\"totalTransactionAmount\":\"5\"}]";

  @Container
  static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("apache/kafka:3.8.1"))
      .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
      .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
      .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1");

  static final Path WORK;
  static final Path INPUT;
  static final Path OUTPUT;
  static final Path CHECKPOINT;
  static final Path STATE;

  static {
    try {
      WORK = Files.createTempDirectory("amn-kafka-it");
      INPUT = WORK.resolve("Input.txt");
      OUTPUT = WORK.resolve("Output.csv");
      CHECKPOINT = WORK.resolve("Input.offset");
      STATE = WORK.resolve("streams-state");
      Files.createDirectories(STATE);
      Files.writeString(INPUT, RECORD_1 + "\n", StandardCharsets.UTF_8);
    } catch (java.io.IOException failure) {
      throw new ExceptionInInitializerError(failure);
    }
  }

  @DynamicPropertySource
  static void register(DynamicPropertyRegistry registry) {
    registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    registry.add("spring.kafka.streams.application-id", () -> APP_ID);
    registry.add("spring.kafka.streams.properties.state.dir", () -> STATE.toAbsolutePath().toString());
    registry.add("spring.kafka.streams.properties.replication.factor", () -> "1");
    registry.add("amn.input-file", () -> INPUT.toAbsolutePath().toString());
    registry.add("amn.output-file", () -> OUTPUT.toAbsolutePath().toString());
    registry.add("amn.checkpoint-file", () -> CHECKPOINT.toAbsolutePath().toString());
    registry.add("amn.topic", () -> TOPIC);
    registry.add("amn.dlq-topic", () -> DLQ);
    registry.add("amn.poll-ms", () -> "50");
  }

  @Autowired TestRestTemplate http;
  @Autowired StreamsBuilderFactoryBean streams;
  @Autowired FileIngestionService ingestion;

  @Test
  @Order(1)
  @Timeout(value = 3, unit = TimeUnit.MINUTES)
  void startupIngestAppendDedupDlqAndLiveHttpAgree() throws Exception {
    assertEquals("exactly_once_v2", liveProcessingGuarantee());
    awaitStreamsRunning();

    String csv = awaitHttp("/api/summary.csv", CSV_AFTER_STARTUP::equals);
    assertEquals(JSON_AFTER_STARTUP, http.getForObject("/api/summary", String.class));
    assertEquals(csv, Files.readString(OUTPUT, StandardCharsets.UTF_8));

    ingestion.poll();
    republishFirst(TOPIC);
    Thread.sleep(500);
    assertEquals(CSV_AFTER_STARTUP, http.getForObject("/api/summary.csv", String.class));

    Files.writeString(INPUT, RECORD_2 + "\n", StandardCharsets.UTF_8, StandardOpenOption.APPEND);
    awaitHttp("/api/summary.csv", CSV_AFTER_APPEND::equals);

    Files.writeString(INPUT, MALFORMED + "\n", StandardCharsets.UTF_8, StandardOpenOption.APPEND);
    String dlq = awaitDlq();
    assertTrue(dlq.contains("\"sourcePosition\":354"), dlq);
    assertTrue(dlq.contains("record code must be 315"), dlq);
    assertEquals(CSV_AFTER_APPEND, http.getForObject("/api/summary.csv", String.class));

    Files.writeString(INPUT, RECORD_3 + "\n", StandardCharsets.UTF_8, StandardOpenOption.APPEND);
    awaitHttp("/api/summary.csv", CSV_FINAL::equals);
    assertEquals(JSON_FINAL, http.getForObject("/api/summary", String.class));
    ResponseEntity<String> csvResponse = http.getForEntity("/api/summary.csv", String.class);
    assertEquals(CSV_FINAL, csvResponse.getBody());
    assertEquals(CSV_FINAL, Files.readString(OUTPUT, StandardCharsets.UTF_8));
    assertTrue(String.valueOf(csvResponse.getHeaders().getContentType()).contains("text/csv"));
  }

  @Test
  @Order(2)
  @Timeout(value = 3, unit = TimeUnit.MINUTES)
  void restartDoesNotDoubleCountPreviouslyIngestedRecords() throws Exception {
    assertEquals("exactly_once_v2", liveProcessingGuarantee());
    awaitStreamsRunning();
    awaitHttp("/api/summary.csv", CSV_FINAL::equals);
    assertEquals(JSON_FINAL, http.getForObject("/api/summary", String.class));
    ingestion.poll();
    Thread.sleep(500);
    assertEquals(CSV_FINAL, http.getForObject("/api/summary.csv", String.class));
    assertEquals(CSV_FINAL, Files.readString(OUTPUT, StandardCharsets.UTF_8));
  }

  private String liveProcessingGuarantee() {
    Properties config = streams.getStreamsConfiguration();
    Object guarantee = config.get(StreamsConfig.PROCESSING_GUARANTEE_CONFIG);
    if (guarantee == null) guarantee = config.getProperty(StreamsConfig.PROCESSING_GUARANTEE_CONFIG);
    return String.valueOf(guarantee);
  }

  private void awaitStreamsRunning() throws InterruptedException {
    await("Kafka Streams RUNNING", ignored -> {
      KafkaStreams kafkaStreams = streams.getKafkaStreams();
      return kafkaStreams != null && kafkaStreams.state() == KafkaStreams.State.RUNNING;
    }, Duration.ofSeconds(90));
  }

  private String awaitHttp(String path, Predicate<String> ready) throws InterruptedException {
    return await(path, last -> {
      String body = http.getForObject(path, String.class);
      return body != null && ready.test(body) ? body : null;
    }, Duration.ofSeconds(90));
  }

  private String awaitDlq() throws InterruptedException {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "amn-it-dlq-" + UUID.randomUUID());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
      consumer.subscribe(List.of(DLQ));
      return await("DLQ", last -> {
        for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(200))) {
          if (record.value() != null && record.value().contains("record code must be 315")) {
            return record.value();
          }
        }
        return null;
      }, Duration.ofSeconds(30));
    }
  }

  private void republishFirst(String topic) throws Exception {
    Properties consumerProps = new Properties();
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "amn-it-redeliver-" + UUID.randomUUID());
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    ConsumerRecord<String, String> first;
    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
      consumer.subscribe(List.of(topic));
      first = await("first transaction", last -> {
        for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(200))) {
          if (record.value() != null && record.value().contains("ABC")) return record;
        }
        return null;
      }, Duration.ofSeconds(15));
    }
    Properties producerProps = new Properties();
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
    try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
      producer.send(new ProducerRecord<>(topic, first.key(), first.value())).get(10, TimeUnit.SECONDS);
    }
  }

  private static <T> T await(String what, java.util.function.Function<T, T> poll, Duration timeout)
      throws InterruptedException {
    long deadline = System.nanoTime() + timeout.toNanos();
    T last = null;
    while (System.nanoTime() < deadline) {
      last = poll.apply(last);
      if (last instanceof Boolean ready) {
        if (ready) return last;
      } else if (last != null && !(last instanceof Boolean)) {
        return last;
      }
      Thread.sleep(100);
    }
    throw new AssertionError("timed out waiting for " + what + ", last=" + last);
  }

  static String record315(String clientType, String clientNumber, String account, String subaccount,
      String productGroup, String exchange, String symbol, String expiration,
      String longQuantity, String shortQuantity) {
    char[] chars = " ".repeat(176).toCharArray();
    put(chars, 1, "315");
    put(chars, 4, clientType);
    put(chars, 8, clientNumber);
    put(chars, 12, account);
    put(chars, 16, subaccount);
    put(chars, 26, productGroup);
    put(chars, 28, exchange);
    put(chars, 32, symbol);
    put(chars, 38, expiration);
    chars[51] = '+';
    put(chars, 53, longQuantity);
    chars[62] = '+';
    put(chars, 64, shortQuantity);
    return new String(chars);
  }

  private static void put(char[] chars, int from1, String value) {
    value.getChars(0, value.length(), chars, from1 - 1);
  }
}
