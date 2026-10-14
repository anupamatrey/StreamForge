package com.anupam.streamforge.streams.model;

import java.util.Comparator;
import java.util.List;

public record CustomerDocument(
        Long customerId,
        String firstName,
        String lastName,
        String email,
        String createdAt,
        List<Address> addresses
) {
    public static CustomerDocument from(Customer customer, AddressCollection collection) {
        List<Address> addresses = collection == null
                ? List.of()
                : collection.getAddresses().values().stream()
                        .sorted(Comparator.comparing(Address::addressId))
                        .toList();

        return new CustomerDocument(
                customer.customerId(),
                customer.firstName(),
                customer.lastName(),
                customer.email(),
                customer.createdAt(),
                addresses
        );
    }
}
