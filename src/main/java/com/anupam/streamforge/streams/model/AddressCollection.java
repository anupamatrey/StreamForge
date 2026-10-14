package com.anupam.streamforge.streams.model;

import java.util.LinkedHashMap;
import java.util.Map;

public class AddressCollection {

    private Map<Long, Address> addresses = new LinkedHashMap<>();

    public AddressCollection() {}

    public Map<Long, Address> getAddresses() {
        return addresses;
    }

    public void setAddresses(Map<Long, Address> addresses) {
        this.addresses = addresses == null
                ? new LinkedHashMap<>()
                : new LinkedHashMap<>(addresses);
    }

    public AddressCollection copy() {
        AddressCollection copy = new AddressCollection();
        copy.setAddresses(addresses);
        return copy;
    }

    public AddressCollection with(Address address) {
        AddressCollection copy = copy();
        copy.addresses.put(address.addressId(), address);
        return copy;
    }

    public AddressCollection without(Long addressId) {
        AddressCollection copy = copy();
        copy.addresses.remove(addressId);
        return copy;
    }
}
