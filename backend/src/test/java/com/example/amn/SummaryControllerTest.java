package com.example.amn;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import java.nio.file.Files;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

class SummaryControllerTest {
  @Test void servesExactJsonStringAndCanonicalIntegerCsv() throws Exception {
    var output = Files.createTempFile("summary", ".csv");
    var report = new ReportService(output.toString());
    report.replaceAggregate("CL|1", "P|1", 9007199254740993L);
    MockMvc mvc = MockMvcBuilders.standaloneSetup(new SummaryController(report)).build();
    mvc.perform(get("/api/summary")).andExpect(status().isOk())
        .andExpect(content().json("[{\"clientInformation\":\"CL|1\",\"productInformation\":\"P|1\",\"totalTransactionAmount\":\"9007199254740993\"}]"));
    mvc.perform(get("/api/summary.csv")).andExpect(status().isOk())
        .andExpect(content().contentTypeCompatibleWith(MediaType.valueOf("text/csv")))
        .andExpect(header().string("Content-Disposition", "attachment; filename=\"Output.csv\""))
        .andExpect(content().string("Client_Information,Product_Information,Total_Transaction_Amount\nCL|1,P|1,9007199254740993\n"));
  }
}
