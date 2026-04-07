package dev.rockend.util;

public record KvLookupResult(
        boolean found,
        byte[] value
) {
}
