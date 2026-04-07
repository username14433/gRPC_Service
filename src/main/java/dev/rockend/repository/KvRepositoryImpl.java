package dev.rockend.repository;

import com.fasterxml.jackson.core.type.TypeReference;
import dev.rockend.util.KvEntry;
import dev.rockend.util.KvLookupResult;
import io.tarantool.client.box.TarantoolBoxClient;

import javax.lang.model.type.ReferenceType;
import java.util.*;

public class KvRepositoryImpl implements KvRepository, AutoCloseable {

    private final TarantoolBoxClient client;

    public KvRepositoryImpl(TarantoolBoxClient client) {
        this.client = client;
    }

    @Override
    public void save(String key, byte[] value) {
        client.call("kv_put", Arrays.asList(key, value), Boolean.class).join();
    }

    @Override
    public KvLookupResult findByKey(String key) {
        var response = client.call(
                "kv_get",
                Collections.singletonList(key),
                new TypeReference<List<Map<String, Object>>>() {
                }
        ).join();

        List<Map<String, Object>> rows = response.get();
        if (rows.isEmpty()) {
            throw new IllegalStateException("kv_get returned empty result");
        }

        Map<String, Object> row = rows.getFirst();

        boolean found = Boolean.TRUE.equals(row.get("found"));
        if (!found) {
            return new KvLookupResult(false, null);
        }

        byte[] value = (byte[]) row.get("value");
        return new KvLookupResult(true, value);
    }

    @Override
    public boolean delete(String key) {
        return client.call("kv_delete", Collections.singletonList(key), Boolean.class).join().get().getFirst();
    }

    @Override
    public long count() {
        return client.call("kv_count", Collections.emptyList(), Long.class).join().get().getFirst();
    }

    @Override
    public List<KvEntry> rangeBatch(String keySince, String keyTo, int limit, boolean includeSince) {
        var response = client.call(
                "kv_range_batch",
                Arrays.asList(keySince, keyTo, limit, includeSince),
                new TypeReference<List<List<Map<String, Object>>>>() {
                }
        ).join();

        List<List<Map<String, Object>>> result = response.get();

        if (result.isEmpty()) {
            return Collections.emptyList();
        }

        List<Map<String, Object>> rows = result.getFirst();
        List<KvEntry> entries = new ArrayList<>(rows.size());

        for (Map<String, Object> row : rows) {
            String key = (String) row.get("key");
            byte[] value = row.containsKey("value")
                    ? (byte[]) row.get("value")
                    : null;

            entries.add(new KvEntry(key, value));
        }

        return entries;
    }

    @Override
    public void close() throws Exception {
        client.close();
    }
}
