package com.example.amn;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

@SpringBootTest(properties = {
    "spring.kafka.streams.auto-startup=false",
    "spring.kafka.admin.auto-create=false",
    "amn.input-file=${java.io.tmpdir}/missing-amn-input.txt"
})
class ApplicationContextTest {
  @MockitoBean KafkaTemplate<String, Transaction> kafkaTemplate;
  @Autowired FixedWidthParser parser;
  @Autowired FileIngestionService ingestion;

  @Test void constructsParserAndIngestionBeans() {
    assertNotNull(parser);
    assertNotNull(ingestion);
  }
}
