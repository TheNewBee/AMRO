package com.example.amn;

import java.math.BigInteger;

public record Transaction(String id, String clientInformation, String productInformation,
                          BigInteger delta, long sourcePosition) {}
