package com.anupam.streamforge.streams.model;

public record Address(
        Long addressId,
        Long customerId,
        String street,
        String city,
        String state,
        String postalCode,
        String country
) {}
