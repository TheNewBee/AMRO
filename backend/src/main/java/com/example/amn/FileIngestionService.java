package com.example.amn;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.concurrent.ExecutionException;
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
  private String fingerprint;

  public FileIngestionService(@Value("${amn.input-file:/data/Input.txt}") String input,
      @Value("${amn.checkpoint-file:/data/Input.offset}") String checkpoint,
      FixedWidthParser parser, KafkaTemplate<String, Transaction> kafka,
      @Value("${amn.topic:transactions}") String topic,
      @Value("${amn.dlq-topic:transactions.DLT}") String dlq) {
    this.input = Path.of(input); this.checkpoint = Path.of(checkpoint); this.parser = parser;
    this.kafka = kafka; this.topic = topic; this.dlq = dlq;
    Checkpoint saved = readCheckpoint();
    this.offset = new AtomicLong(saved.offset());
    this.fingerprint = saved.fingerprint();
  }

  @Scheduled(fixedDelayString = "${amn.poll-ms:1000}")
  public synchronized void poll() {
    if (!Files.exists(input)) return;
    long startingOffset = offset.get();
    try {
      long size = Files.size(input);
      if (size < startingOffset || fingerprint != null && !fingerprint.equals(fingerprint(startingOffset)))
        throw new IllegalStateException("input truncation/rewrite detected");
      try (RandomAccessFile file = new RandomAccessFile(input.toFile(), "r")) {
        file.seek(startingOffset);
        while (true) {
          long position = file.getFilePointer();
          String line = file.readLine();
          if (line == null) break;
          long next = file.getFilePointer();
          String record = new String(line.getBytes(StandardCharsets.ISO_8859_1), StandardCharsets.UTF_8);
          if (!record.isBlank() && next == size && !endsWithLineTerminator(file, next)
              && record.length() != 176 && record.length() != 303) {
            file.seek(position);
            break;
          }
          if (!record.isBlank()) {
            Transaction tx;
            try {
              tx = parser.parse(record, position);
            } catch (IllegalArgumentException parseFailure) {
              sendAndAwait(dlq, String.valueOf(position),
                  new Transaction(String.valueOf(position), record, parseFailure.getMessage(),
                      java.math.BigInteger.ZERO, position));
              offset.set(next);
              persistCheckpoint();
              continue;
            }
            sendAndAwait(topic, tx.id(), tx);
          }
          offset.set(next);
          persistCheckpoint();
        }
      }
      persistCheckpoint();
    } catch (IOException failure) {
      persistProgress(startingOffset, failure);
      throw new IllegalStateException("input ingestion failed", failure);
    } catch (RuntimeException failure) {
      persistProgress(startingOffset, failure);
      throw failure;
    }
  }

  private void sendAndAwait(String destination, String key, Transaction transaction) {
    try {
      kafka.send(destination, key, transaction).get();
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Kafka send interrupted", interrupted);
    } catch (ExecutionException failure) {
      throw new IllegalStateException("Kafka send failed", failure.getCause());
    }
  }

  private boolean endsWithLineTerminator(RandomAccessFile file, long end) throws IOException {
    if (end == 0) return false;
    file.seek(end - 1);
    int last = file.read();
    file.seek(end);
    return last == '\n' || last == '\r';
  }

  private void persistProgress(long startingOffset, Exception original) {
    if (offset.get() == startingOffset) return;
    try { persistCheckpoint(); }
    catch (IOException checkpointFailure) { original.addSuppressed(checkpointFailure); }
  }

  private void persistCheckpoint() throws IOException {
    fingerprint = fingerprint(offset.get());
    Path parent = checkpoint.toAbsolutePath().getParent();
    if (parent != null) Files.createDirectories(parent);
    Files.writeString(checkpoint, offset.get() + "\n" + fingerprint + "\n", StandardCharsets.UTF_8,
        StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
  }

  private String fingerprint(long length) throws IOException {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      try (var stream = Files.newInputStream(input)) {
        byte[] buffer = new byte[8192];
        long remaining = length;
        while (remaining > 0) {
          int read = stream.read(buffer, 0, (int) Math.min(buffer.length, remaining));
          if (read < 0) throw new IOException("input shorter than checkpoint");
          digest.update(buffer, 0, read);
          remaining -= read;
        }
      }
      return HexFormat.of().formatHex(digest.digest());
    } catch (java.security.NoSuchAlgorithmException impossible) {
      throw new IllegalStateException("SHA-256 unavailable", impossible);
    }
  }

  private Checkpoint readCheckpoint() {
    try {
      if (!Files.exists(checkpoint)) return new Checkpoint(0, null);
      String[] lines = Files.readString(checkpoint).trim().split("\\R", 2);
      long savedOffset = Long.parseLong(lines[0]);
      return new Checkpoint(savedOffset,
          lines.length == 2 ? lines[1].trim() : Files.exists(input) ? fingerprint(savedOffset) : null);
    } catch (Exception ignored) {
      return new Checkpoint(0, null);
    }
  }

  private record Checkpoint(long offset, String fingerprint) {}
}
