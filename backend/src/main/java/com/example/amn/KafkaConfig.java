package com.example.amn;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

@Configuration(proxyBeanMethods = false)
public class KafkaConfig {
  @Bean NewTopic transactionsTopic(@Value("${amn.topic:transactions}") String name) { return new NewTopic(name, 1, (short) 1); }
  @Bean NewTopic deadLetterTopic(@Value("${amn.dlq-topic:transactions.DLT}") String name) { return new NewTopic(name, 1, (short) 1); }
  @Bean ProducerFactory<String, Transaction> producerFactory(KafkaProperties properties) {
    Map<String, Object> config = new HashMap<>(properties.buildProducerProperties());
    config.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);
    return new DefaultKafkaProducerFactory<>(config, new StringSerializer(), new JsonSerializer<>());
  }
  @Bean KafkaTemplate<String, Transaction> kafkaTemplate(ProducerFactory<String, Transaction> factory) { return new KafkaTemplate<>(factory); }
}
