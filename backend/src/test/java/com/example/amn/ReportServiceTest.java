package com.example.amn;

import static org.junit.jupiter.api.Assertions.assertEquals;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;

class ReportServiceTest {
  @Test void sortsAndWritesCanonicalCsv() throws Exception {
    Path output = Files.createTempFile("report", ".csv");
    ReportService report = new ReportService(output.toString());
    report.replaceAggregate("CL|2", "P|2", BigInteger.TEN);
    report.replaceAggregate("CL|1", "P|1", BigInteger.valueOf(-2));
    assertEquals(2, report.rows().size());
    assertEquals("CL|1", report.rows().get(0).clientInformation());
    assertEquals("Client_Information,Product_Information,Total_Transaction_Amount\nCL|1,P|1,-2\nCL|2,P|2,10\n", Files.readString(output));
  }

  @Test void replaceAllReplacesTheSnapshot() throws Exception {
    Path output = Files.createTempFile("report-restore", ".csv");
    ReportService report = new ReportService(output.toString());
    report.replaceAggregate("stale", "row", BigInteger.ONE);
    report.replaceAll(List.of(new ReportService.Aggregate("CL|9", "P|9", BigInteger.valueOf(42))));
    assertEquals("Client_Information,Product_Information,Total_Transaction_Amount\nCL|9,P|9,42\n", Files.readString(output));
  }
}
