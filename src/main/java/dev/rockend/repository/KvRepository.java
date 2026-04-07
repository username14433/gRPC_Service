package dev.rockend.repository;


import dev.rockend.util.KvEntry;
import dev.rockend.util.KvLookupResult;

import java.util.List;

public interface KvRepository {

    KvLookupResult findByKey(String key);

    void save(String key, byte[] value);

    boolean delete(String key);

    long count();

    List<KvEntry> rangeBatch(String keySince, String keyTo, int limit, boolean includeSince);
}
