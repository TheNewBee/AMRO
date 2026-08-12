package com.example.amn;

import static org.junit.jupiter.api.Assertions.assertEquals;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class ReportServiceTest {
  @Test void deduplicatesSortsAndWritesCanonicalCsv() throws Exception {
    Path output = Files.createTempFile("report", ".csv");
    ReportService report = new ReportService(output.toString());
    report.accept(new Transaction("a", "CL|2", "P|2", BigInteger.TEN, 0));
    report.accept(new Transaction("b", "CL|1", "P|1", BigInteger.valueOf(-2), 1));
    report.accept(new Transaction("a", "CL|2", "P|2", BigInteger.TEN, 0));
    assertEquals(2, report.rows().size());
    assertEquals("CL|1", report.rows().get(0).clientInformation());
    assertEquals("Client_Information,Product_Information,Total_Transaction_Amount\nCL|1,P|1,-2\nCL|2,P|2,10\n", Files.readString(output));
  }
}
