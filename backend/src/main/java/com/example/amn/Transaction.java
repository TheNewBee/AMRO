package com.example.amn;

public record Transaction(String id, String clientInformation, String productInformation,
                          long delta, long sourcePosition) {}
