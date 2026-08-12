package com.example.amn;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component("legacyTransactionListener")
@org.springframework.context.annotation.Profile("legacy-listener")
public class TransactionListener {
  private final ReportService report;
  public TransactionListener(ReportService report) { this.report = report; }
  @KafkaListener(topics = "${amn.topic:transactions}", groupId = "${amn.consumer-group:amn-report}")
  public void receive(Transaction transaction) { report.accept(transaction); }
}
