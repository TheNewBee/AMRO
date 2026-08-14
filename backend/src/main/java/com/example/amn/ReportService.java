package com.example.amn;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class ReportService {
  public record Row(String clientInformation, String productInformation, String totalTransactionAmount) {}
  public record Aggregate(String client, String product, BigInteger total) {}
  private record Key(String client, String product) {}
  private final Map<Key, BigInteger> totals = new ConcurrentHashMap<>();
  private final Path output;

  public ReportService(@Value("${amn.output-file:/data/Output.csv}") String output) { this.output = Path.of(output); }

  public synchronized void replaceAggregate(String client, String product, BigInteger total) {
    totals.put(new Key(client, product), total);
    writeAtomically();
  }

  public synchronized void replaceAll(Collection<Aggregate> restored) {
    totals.clear();
    for (Aggregate aggregate : restored) totals.put(new Key(aggregate.client(), aggregate.product()), aggregate.total());
    writeAtomically();
  }

  public synchronized List<Row> rows() {
    return totals.entrySet().stream()
        .map(entry -> new Row(entry.getKey().client(), entry.getKey().product(), entry.getValue().toString()))
        .sorted(Comparator.comparing(Row::clientInformation).thenComparing(Row::productInformation))
        .toList();
  }

  public synchronized String csv() {
    StringBuilder csv = new StringBuilder("Client_Information,Product_Information,Total_Transaction_Amount\n");
    for (Row row : rows()) csv.append(escape(row.clientInformation())).append(',').append(escape(row.productInformation())).append(',').append(row.totalTransactionAmount()).append('\n');
    return csv.toString();
  }

  private void writeAtomically() {
    try {
      Path parent = output.toAbsolutePath().getParent();
      Files.createDirectories(parent);
      Path temporary = Files.createTempFile(parent, "Output-", ".tmp");
      try {
        Files.writeString(temporary, csv(), StandardCharsets.UTF_8);
        try { Files.move(temporary, output, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING); }
        catch (AtomicMoveNotSupportedException e) { Files.move(temporary, output, StandardCopyOption.REPLACE_EXISTING); }
      } finally { Files.deleteIfExists(temporary); }
    } catch (IOException e) { throw new IllegalStateException("cannot write report", e); }
  }

  private static String escape(String value) { return value.contains(",") || value.contains("\"") || value.contains("\n") ? '"' + value.replace("\"", "\"\"") + '"' : value; }
}
