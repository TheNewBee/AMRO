package com.example.amn;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;

class FileIngestionServiceTest {
  @SuppressWarnings("unchecked")
  private static KafkaTemplate<String, Transaction> kafka() {
    KafkaTemplate<String, Transaction> kafka = mock(KafkaTemplate.class);
    when(kafka.send(anyString(), anyString(), any(Transaction.class)))
        .thenReturn(CompletableFuture.completedFuture(null));
    return kafka;
  }

  private static FileIngestionService service(Path input, Path checkpoint,
      FixedWidthParser parser, KafkaTemplate<String, Transaction> kafka) {
    return new FileIngestionService(input.toString(), checkpoint.toString(), parser, kafka,
        "transactions", "transactions.DLT");
  }

  @Test void processesCompleteFinalRecordWithoutNewlineAndDefersIncompleteTail() throws Exception {
    Path directory = Files.createTempDirectory("ingestion-eof");
    Path input = directory.resolve("Input.txt"), checkpoint = directory.resolve("Input.offset");
    String complete = validRecord();
    Files.writeString(input, complete, StandardCharsets.UTF_8);
    KafkaTemplate<String, Transaction> kafka = kafka();
    service(input, checkpoint, new FixedWidthParser(), kafka).poll();
    verify(kafka).send(eq("transactions"), anyString(), any(Transaction.class));
    assertEquals(complete.length(), savedOffset(checkpoint));

    Files.delete(checkpoint);
    Files.writeString(input, complete.substring(0, 100), StandardCharsets.UTF_8);
    kafka = kafka();
    service(input, checkpoint, new FixedWidthParser(), kafka).poll();
    verifyNoInteractions(kafka);
    assertEquals(0, savedOffset(checkpoint));
  }

  @Test void transportFailureDoesNotGoToDlqOrAdvanceFailedRecord() throws Exception {
    Path directory = Files.createTempDirectory("ingestion-send-failure");
    Path input = directory.resolve("Input.txt"), checkpoint = directory.resolve("Input.offset");
    Files.writeString(input, validRecord() + "\n", StandardCharsets.UTF_8);
    KafkaTemplate<String, Transaction> kafka = kafka();
    when(kafka.send(eq("transactions"), anyString(), any(Transaction.class)))
        .thenReturn(CompletableFuture.failedFuture(new RuntimeException("broker unavailable")));
    assertThrows(IllegalStateException.class,
        () -> service(input, checkpoint, new FixedWidthParser(), kafka).poll());
    verify(kafka, never()).send(eq("transactions.DLT"), anyString(), any(Transaction.class));
    assertFalse(Files.exists(checkpoint));
  }

  @Test void interruptionPreservesThreadFlagAndCheckpoint() throws Exception {
    Path directory = Files.createTempDirectory("ingestion-interrupt");
    Path input = directory.resolve("Input.txt"), checkpoint = directory.resolve("Input.offset");
    Files.writeString(input, validRecord() + "\n", StandardCharsets.UTF_8);
    KafkaTemplate<String, Transaction> kafka = kafka();
    CompletableFuture<Object> interrupted = new CompletableFuture<>() {
      @Override public Object get() throws InterruptedException { throw new InterruptedException("stop"); }
    };
    when(kafka.send(eq("transactions"), anyString(), any(Transaction.class)))
        .thenReturn((CompletableFuture) interrupted);
    try {
      assertThrows(IllegalStateException.class,
          () -> service(input, checkpoint, new FixedWidthParser(), kafka).poll());
      assertTrue(Thread.currentThread().isInterrupted());
      assertFalse(Files.exists(checkpoint));
    } finally {
      Thread.interrupted();
    }
  }

  @Test void parseFailureAdvancesOnlyAfterDlqAcknowledgement() throws Exception {
    Path directory = Files.createTempDirectory("ingestion-dlq");
    Path input = directory.resolve("Input.txt"), checkpoint = directory.resolve("Input.offset");
    String malformed = "x".repeat(176);
    Files.writeString(input, malformed + "\n", StandardCharsets.UTF_8);
    KafkaTemplate<String, Transaction> kafka = kafka();
    service(input, checkpoint, new FixedWidthParser(), kafka).poll();
    verify(kafka).send(eq("transactions.DLT"), eq("0"), any(Transaction.class));
    assertEquals(177, savedOffset(checkpoint));

    Files.delete(checkpoint);
    KafkaTemplate<String, Transaction> failedDlq = kafka();
    when(failedDlq.send(eq("transactions.DLT"), anyString(), any(Transaction.class)))
        .thenReturn(CompletableFuture.failedFuture(new RuntimeException("DLQ unavailable")));
    assertThrows(IllegalStateException.class,
        () -> service(input, checkpoint, new FixedWidthParser(), failedDlq).poll());
    assertFalse(Files.exists(checkpoint));
  }

  @Test void checkpointsAcknowledgedPrefixBeforeLaterFailure() throws Exception {
    Path directory = Files.createTempDirectory("ingestion-partial");
    Path input = directory.resolve("Input.txt"), checkpoint = directory.resolve("Input.offset");
    String record = validRecord();
    Files.writeString(input, record + "\n" + record + "\n", StandardCharsets.UTF_8);
    KafkaTemplate<String, Transaction> kafka = kafka();
    when(kafka.send(eq("transactions"), anyString(), any(Transaction.class)))
        .thenReturn(CompletableFuture.completedFuture(null))
        .thenReturn(CompletableFuture.failedFuture(new RuntimeException("second failed")));
    assertThrows(IllegalStateException.class,
        () -> service(input, checkpoint, new FixedWidthParser(), kafka).poll());
    assertEquals(record.length() + 1, savedOffset(checkpoint));
  }

  @Test void detectsSameSizeAndLargerPrefixRewrites() throws Exception {
    for (boolean larger : new boolean[] {false, true}) {
      Path directory = Files.createTempDirectory("ingestion-rewrite");
      Path input = directory.resolve("Input.txt"), checkpoint = directory.resolve("Input.offset");
      String original = validRecord();
      Files.writeString(input, original + "\n", StandardCharsets.UTF_8);
      service(input, checkpoint, new FixedWidthParser(), kafka()).poll();
      String rewritten = "x" + original.substring(1) + "\n" + (larger ? "tail" : "");
      Files.writeString(input, rewritten, StandardCharsets.UTF_8);
      assertThrows(IllegalStateException.class,
          () -> service(input, checkpoint, new FixedWidthParser(), kafka()).poll());
    }
  }

  @Test void appendAfterFingerprintHeadIsNotARewrite() throws Exception {
    Path directory = Files.createTempDirectory("ingestion-append-head");
    Path input = directory.resolve("Input.txt"), checkpoint = directory.resolve("Input.offset");
    String record = validRecord() + "\n";
    int copies = FileIngestionService.FINGERPRINT_HEAD / record.length() + 3;
    Files.writeString(input, record.repeat(copies), StandardCharsets.UTF_8);
    KafkaTemplate<String, Transaction> kafka = kafka();
    FileIngestionService ingestion = service(input, checkpoint, new FixedWidthParser(), kafka);
    ingestion.poll();
    verify(kafka, times(copies)).send(eq("transactions"), anyString(), any(Transaction.class));
    Files.writeString(input, record, StandardCharsets.UTF_8, StandardOpenOption.APPEND);
    ingestion.poll();
    verify(kafka, times(copies + 1)).send(eq("transactions"), anyString(), any(Transaction.class));
  }

  @Test void migratesLegacyFullPrefixFingerprint() throws Exception {
    Path directory = Files.createTempDirectory("ingestion-migrate");
    Path input = directory.resolve("Input.txt"), checkpoint = directory.resolve("Input.offset");
    Files.writeString(input, validRecord() + "\n", StandardCharsets.UTF_8);
    service(input, checkpoint, new FixedWidthParser(), kafka()).poll();
    Files.writeString(checkpoint, savedOffset(checkpoint) + "\nold-full-prefix-hash\n");
    KafkaTemplate<String, Transaction> kafka = kafka();
    service(input, checkpoint, new FixedWidthParser(), kafka).poll();
    verifyNoInteractions(kafka);
  }

  static String validRecord() {
    char[] chars = " ".repeat(176).toCharArray();
    chars[0] = '3'; chars[1] = '1'; chars[2] = '5';
    chars[51] = '+';
    System.arraycopy("0000000001".toCharArray(), 0, chars, 52, 10);
    chars[62] = '+';
    System.arraycopy("0000000000".toCharArray(), 0, chars, 63, 10);
    return new String(chars);
  }

  private static long savedOffset(Path checkpoint) throws Exception {
    return Long.parseLong(Files.readString(checkpoint).split("\\R", 2)[0]);
  }
}
