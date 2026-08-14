package com.example.amn;

import static org.junit.jupiter.api.Assertions.*;
import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.Test;

class FixedWidthParserTest {
  @Test void suppliedInputProducesFiveRows() throws Exception {
    FixedWidthParser p=new FixedWidthParser(); Map<String,Long> totals=new HashMap<>(); long pos=0; int count=0;
    for(String line:Files.readAllLines(Path.of(System.getProperty("amn.input", "../Input.txt")))) { Transaction t=p.parse(line,pos++); totals.merge(t.clientInformation()+","+t.productInformation(),t.delta(),Long::sum); count++; }
    assertEquals(717,count);
    assertEquals(Map.of("CL|1234|0002|0001,SGX|FU|NK|20100910",-52L,"CL|1234|0003|0001,CME|FU|N1|20100910",285L,"CL|1234|0003|0001,CME|FU|NK.|20100910",-215L,"CL|4321|0002|0001,SGX|FU|NK|20100910",46L,"CL|4321|0003|0001,CME|FU|N1|20100910",-79L),totals);
  }
  @Test void supports303AndSigns() {
    char[] chars = " ".repeat(303).toCharArray();
    chars[0] = '3'; chars[1] = '1'; chars[2] = '5';
    System.arraycopy("0000000001".toCharArray(), 0, chars, 52, 10);
    chars[62] = '-';
    System.arraycopy("0000000000".toCharArray(), 0, chars, 63, 10);
    Transaction parsed = new FixedWidthParser().parse(new String(chars), 1);
    assertEquals(1L, parsed.delta());
    assertEquals("1", parsed.id());
  }

  @Test void rejectsInvalid() { assertThrows(IllegalArgumentException.class,()->new FixedWidthParser().parse("x",0)); }
}
