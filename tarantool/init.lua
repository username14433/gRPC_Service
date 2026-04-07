box.cfg{
    listen = '0.0.0.0:3301'
}

box.once('bootstrap_kv', function()
    local kv = box.schema.space.create('KV', {
        if_not_exists = true
    })

    kv:format({
        {name = 'key', type = 'string'},
        {name = 'value', type = 'varbinary', is_nullable = true}
    })

    kv:create_index('primary', {
        type = 'TREE',
        unique = true,
        parts = {
            {field = 'key', type = 'string'}
        },
        if_not_exists = true
    })

    if not box.schema.user.exists('app') then
        box.schema.user.create('app', { password = 'app' })
    end

    box.schema.user.grant('app', 'read,write', 'space', 'KV', { if_not_exists = true })
    box.schema.user.grant('app', 'execute', 'universe', nil, { if_not_exists = true })
end)

function kv_put(key, value)
    box.space.KV:replace({key, value})
    return true
end

function kv_get(key)
    local tuple = box.space.KV:get({key})
    if tuple == nil then
        return { found = false }
    end

    if tuple[2] == nil then
        return { found = true }
    end

    return {
        found = true,
        value = tuple[2]
    }
end

function kv_delete(key)
    local deleted = box.space.KV:delete({key})
    return deleted ~= nil
end

function kv_count()
    return box.space.KV:count()
end

function kv_range_batch(key_since, key_to, limit, include_since)
    local result = {};
    local iterator = 'GE';

    if not include_since then
        iterator = 'GT'
    end

    for _, tuple in box.space.KV.index.primary:pairs({key_since}, {iterator = iterator}) do
        local key = tuple[1]
        if key >= key_to then
            break
        end

        table.insert(result, {
            key = key,
            value = tuple[2]
        })

        if #result >= limit then
            break
        end
    end

    return result
end