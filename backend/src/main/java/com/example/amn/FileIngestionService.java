package com.example.amn;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
public class FileIngestionService {
  private final Path input, checkpoint;
  private final FixedWidthParser parser;
  private final KafkaTemplate<String, Transaction> kafka;
  private final String topic, dlq;
  private final AtomicLong offset;

  public FileIngestionService(@Value("${amn.input-file:/data/Input.txt}") String input,
      @Value("${amn.checkpoint-file:/data/Input.offset}") String checkpoint,
      FixedWidthParser parser, KafkaTemplate<String, Transaction> kafka,
      @Value("${amn.topic:transactions}") String topic,
      @Value("${amn.dlq-topic:transactions.DLT}") String dlq) {
    this.input = Path.of(input); this.checkpoint = Path.of(checkpoint); this.parser = parser;
    this.kafka = kafka; this.topic = topic; this.dlq = dlq; this.offset = new AtomicLong(readOffset());
  }

  @Scheduled(fixedDelayString = "${amn.poll-ms:1000}")
  public synchronized void poll() {
    if (!Files.exists(input)) return;
    try {
      long size = Files.size(input);
      if (size < offset.get()) throw new IllegalStateException("input truncation/rewrite detected");
      try (RandomAccessFile file = new RandomAccessFile(input.toFile(), "r")) {
        file.seek(offset.get());
        while (true) {
          long position = file.getFilePointer();
          String line = file.readLine();
          if (line == null) break;
          long next = file.getFilePointer();
          if (next == size && !endsWithLineTerminator(file, next)) { file.seek(position); break; }
          offset.set(next);
          String record = new String(line.getBytes(StandardCharsets.ISO_8859_1), StandardCharsets.UTF_8);
          if (record.isBlank()) continue;
          try {
            Transaction tx = parser.parse(record, position);
            kafka.send(topic, tx.id(), tx);
          } catch (Exception e) {
            kafka.send(dlq, String.valueOf(position), new Transaction(String.valueOf(position), record, e.getMessage(), java.math.BigInteger.ZERO, position));
          }
        }
      }
      Path parent = checkpoint.toAbsolutePath().getParent();
      if (parent != null) Files.createDirectories(parent);
      Files.writeString(checkpoint, Long.toString(offset.get()), StandardCharsets.UTF_8,
          StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
    } catch (IOException e) { throw new IllegalStateException("input ingestion failed", e); }
  }

  private boolean endsWithLineTerminator(RandomAccessFile file, long end) throws IOException {
    if (end == 0) return false;
    file.seek(end - 1);
    int last = file.read();
    if (last == '\n') return true;
    if (last == '\r') return true;
    return last == '\n' || last == '\r';
  }

  private long readOffset() {
    try { return Files.exists(checkpoint) ? Long.parseLong(Files.readString(checkpoint).trim()) : 0; }
    catch (Exception e) { return 0; }
  }
}
