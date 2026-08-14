package com.example.amn;

import org.springframework.stereotype.Component;

@Component
public final class FixedWidthParser {
  private static String field(String value, int from, int to) { return value.substring(from - 1, to).trim(); }

  public Transaction parse(String raw, long sourcePosition) {
    if (raw == null || raw.isBlank()) throw new IllegalArgumentException("blank record");
    if (raw.length() != 176 && raw.length() != 303) throw new IllegalArgumentException("record length must be 176 or 303");
    if (!raw.startsWith("315")) throw new IllegalArgumentException("record code must be 315");
    String client = String.join("|", field(raw, 4, 7), field(raw, 8, 11), field(raw, 12, 15), field(raw, 16, 19));
    String product = String.join("|", field(raw, 28, 31), field(raw, 26, 27), field(raw, 32, 37), field(raw, 38, 45));
    long longQuantity = signedNumber(field(raw, 52, 52), field(raw, 53, 62));
    long shortQuantity = signedNumber(field(raw, 63, 63), field(raw, 64, 73));
    return new Transaction(Long.toString(sourcePosition), client, product, longQuantity - shortQuantity, sourcePosition);
  }

  private static long signedNumber(String sign, String value) {
    if (!(sign.isEmpty() || sign.equals("+") || sign.equals("-"))) throw new IllegalArgumentException("invalid quantity sign");
    if (value.isEmpty() || !digits(value)) throw new IllegalArgumentException("invalid quantity");
    long number = Long.parseLong(value);
    return sign.equals("-") ? -number : number;
  }

  private static boolean digits(String value) {
    for (int i = 0, n = value.length(); i < n; i++) {
      char c = value.charAt(i);
      if (c < '0' || c > '9') return false;
    }
    return true;
  }
}
