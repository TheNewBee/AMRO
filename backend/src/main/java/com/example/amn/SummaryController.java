package com.example.amn;

import java.nio.charset.StandardCharsets;
import java.util.List;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api")
public class SummaryController {
  private final ReportService report;
  public SummaryController(ReportService report) { this.report=report; }
  @GetMapping("/summary") public List<ReportService.Row> summary() { return report.rows(); }
  @GetMapping(value="/summary.csv", produces="text/csv") public ResponseEntity<byte[]> csv() { return ResponseEntity.ok().contentType(MediaType.parseMediaType("text/csv")).header(HttpHeaders.CONTENT_DISPOSITION,"attachment; filename=\"Output.csv\"").body(report.csv().getBytes(StandardCharsets.UTF_8)); }
}
