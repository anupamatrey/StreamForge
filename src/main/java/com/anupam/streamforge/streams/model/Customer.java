package com.anupam.streamforge.streams.model;

public record Customer(
        Long customerId,
        String firstName,
        String lastName,
        String email,
        String createdAt
) {}
