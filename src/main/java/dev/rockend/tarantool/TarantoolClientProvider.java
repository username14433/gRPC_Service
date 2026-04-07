package dev.rockend.tarantool;

import io.tarantool.client.box.TarantoolBoxClient;
import io.tarantool.client.factory.TarantoolFactory;
import io.tarantool.pool.InstanceConnectionGroup;

import java.util.List;

public final class TarantoolClientProvider {

    private TarantoolClientProvider() {
    }

    public static TarantoolBoxClient create(String host, int port, String user, String password) throws Exception {
        InstanceConnectionGroup group = InstanceConnectionGroup.builder()
                .withHost(host)
                .withPort(port)
                .withUser(user)
                .withPassword(password)
                .build();

        return TarantoolFactory.box()
                .withGroups(List.of(group))
                .build();
    }
}