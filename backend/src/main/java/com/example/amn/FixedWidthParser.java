package com.example.amn;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;

public final class FixedWidthParser {
  private static String field(String value, int from, int to) { return value.substring(from - 1, to).trim(); }

  public Transaction parse(String raw, long sourcePosition) {
    if (raw == null || raw.isBlank()) throw new IllegalArgumentException("blank record");
    if (raw.length() != 176 && raw.length() != 303) throw new IllegalArgumentException("record length must be 176 or 303");
    if (!raw.startsWith("315")) throw new IllegalArgumentException("record code must be 315");
    String client = String.join("|", field(raw, 4, 7), field(raw, 8, 11), field(raw, 12, 15), field(raw, 16, 19));
    String product = String.join("|", field(raw, 28, 31), field(raw, 26, 27), field(raw, 32, 37), field(raw, 38, 45));
    BigInteger longQuantity = signedNumber(field(raw, 52, 52), field(raw, 53, 62));
    BigInteger shortQuantity = signedNumber(field(raw, 63, 63), field(raw, 64, 73));
    return new Transaction(hash(sourcePosition + "\n" + raw), client, product, longQuantity.subtract(shortQuantity), sourcePosition);
  }

  private static BigInteger signedNumber(String sign, String value) {
    if (!(sign.isEmpty() || sign.equals("+") || sign.equals("-"))) throw new IllegalArgumentException("invalid quantity sign");
    if (value.isEmpty() || !value.matches("\\d+")) throw new IllegalArgumentException("invalid quantity");
    BigInteger number = new BigInteger(value);
    return sign.equals("-") ? number.negate() : number;
  }

  private static String hash(String value) {
    try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8))); }
    catch (Exception e) { throw new IllegalStateException("SHA-256 unavailable", e); }
  }
}
