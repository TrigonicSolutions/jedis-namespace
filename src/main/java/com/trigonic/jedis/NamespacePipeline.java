package com.trigonic.jedis;

import java.util.List;
import java.util.Map;
import java.util.Set;

import redis.clients.jedis.BinaryClient.LIST_POSITION;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.SortingParams;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.ZParams;

public class NamespacePipeline extends Pipeline {
    private NamespaceHandler namespace;

    public NamespacePipeline(NamespaceHandler namespace) {
        this.namespace = namespace;
    }

    @Override
    public Response<Long> append(byte[] key, byte[] value) {
        return super.append(namespace.add(key), value);
    }

    @Override
    public Response<Long> append(String key, String value) {
        return super.append(namespace.add(key), value);
    }

    @Override
    public Response<List<String>> blpop(byte[]... keys) {
        return namespace.removeFirst(super.blpop(namespace.add(keys)));
    }

    @Override
    public Response<List<String>> blpop(String... keys) {
        return namespace.removeFirst(namespace.removeFirst(super.blpop(namespace.add(keys))));
    }

    @Override
    public Response<List<String>> brpop(byte[]... keys) {
        return namespace.removeFirst(super.brpop(namespace.add(keys)));
    }

    @Override
    public Response<List<String>> brpop(String... keys) {
        return namespace.removeFirst(super.brpop(namespace.add(keys)));
    }

    @Override
    public Response<String> brpoplpush(byte[] source, byte[] destination, int timeout) {
        return super.brpoplpush(namespace.add(source), namespace.add(destination), timeout);
    }

    @Override
    public Response<String> brpoplpush(String source, String destination, int timeout) {
        return super.brpoplpush(namespace.add(source), namespace.add(destination), timeout);
    }

    @Override
    public Response<Long> decr(byte[] key) {
        return super.decr(namespace.add(key));
    }

    @Override
    public Response<Long> decr(String key) {
        return super.decr(namespace.add(key));
    }

    @Override
    public Response<Long> decrBy(byte[] key, long integer) {
        return super.decrBy(namespace.add(key), integer);
    }

    @Override
    public Response<Long> decrBy(String key, long integer) {
        return super.decrBy(namespace.add(key), integer);
    }

    @Override
    public Response<Long> del(byte[]... keys) {
        return super.del(namespace.add(keys));
    }

    @Override
    public Response<Long> del(String... keys) {
        return super.del(namespace.add(keys));
    }

    @Override
    public Response<Boolean> exists(byte[] key) {
        return super.exists(namespace.add(key));
    }

    @Override
    public Response<Boolean> exists(String key) {
        return super.exists(namespace.add(key));
    }

    @Override
    public Response<Long> expire(byte[] key, int seconds) {
        return super.expire(namespace.add(key), seconds);
    }

    @Override
    public Response<Long> expire(String key, int seconds) {
        return super.expire(namespace.add(key), seconds);
    }

    @Override
    public Response<Long> expireAt(byte[] key, long unixTime) {
        return super.expireAt(namespace.add(key), unixTime);
    }

    @Override
    public Response<Long> expireAt(String key, long unixTime) {
        return super.expireAt(namespace.add(key), unixTime);
    }

    @Override
    public Response<byte[]> get(byte[] key) {
        return super.get(namespace.add(key));
    }

    @Override
    public Response<String> get(String key) {
        return super.get(namespace.add(key));
    }

    @Override
    public Response<Boolean> getbit(String key, long offset) {
        return super.getbit(namespace.add(key), offset);
    }

    @Override
    public Response<String> getrange(String key, long startOffset, long endOffset) {
        return super.getrange(namespace.add(key), startOffset, endOffset);
    }

    @Override
    public Response<byte[]> getSet(byte[] key, byte[] value) {
        return super.getSet(namespace.add(key), value);
    }

    @Override
    public Response<String> getSet(String key, String value) {
        return super.getSet(namespace.add(key), value);
    }

    @Override
    public Response<Long> hdel(byte[] key, byte[] field) {
        return super.hdel(namespace.add(key), field);
    }

    @Override
    public Response<Long> hdel(String key, String field) {
        return super.hdel(namespace.add(key), field);
    }

    @Override
    public Response<Boolean> hexists(byte[] key, byte[] field) {
        return super.hexists(namespace.add(key), field);
    }

    @Override
    public Response<Boolean> hexists(String key, String field) {
        return super.hexists(namespace.add(key), field);
    }

    @Override
    public Response<String> hget(byte[] key, byte[] field) {
        return super.hget(namespace.add(key), field);
    }

    @Override
    public Response<String> hget(String key, String field) {
        return super.hget(namespace.add(key), field);
    }

    @Override
    public Response<Map<String, String>> hgetAll(byte[] key) {
        return super.hgetAll(namespace.add(key));
    }

    @Override
    public Response<Map<String, String>> hgetAll(String key) {
        return super.hgetAll(namespace.add(key));
    }

    @Override
    public Response<Long> hincrBy(byte[] key, byte[] field, long value) {
        return super.hincrBy(namespace.add(key), field, value);
    }

    @Override
    public Response<Long> hincrBy(String key, String field, long value) {
        return super.hincrBy(namespace.add(key), field, value);
    }

    @Override
    public Response<Set<String>> hkeys(byte[] key) {
        return super.hkeys(namespace.add(key));
    }

    @Override
    public Response<Set<String>> hkeys(String key) {
        return super.hkeys(namespace.add(key));
    }

    @Override
    public Response<Long> hlen(byte[] key) {
        return super.hlen(namespace.add(key));
    }

    @Override
    public Response<Long> hlen(String key) {
        return super.hlen(namespace.add(key));
    }

    @Override
    public Response<List<String>> hmget(byte[] key, byte[]... fields) {
        return super.hmget(namespace.add(key), fields);
    }

    @Override
    public Response<List<String>> hmget(String key, String... fields) {
        return super.hmget(namespace.add(key), fields);
    }

    @Override
    public Response<String> hmset(byte[] key, Map<byte[], byte[]> hash) {
        return super.hmset(namespace.add(key), hash);
    }

    @Override
    public Response<String> hmset(String key, Map<String, String> hash) {
        return super.hmset(namespace.add(key), hash);
    }

    @Override
    public Response<Long> hset(byte[] key, byte[] field, byte[] value) {
        return super.hset(namespace.add(key), field, value);
    }

    @Override
    public Response<Long> hset(String key, String field, String value) {
        return super.hset(namespace.add(key), field, value);
    }

    @Override
    public Response<Long> hsetnx(byte[] key, byte[] field, byte[] value) {
        return super.hsetnx(namespace.add(key), field, value);
    }

    @Override
    public Response<Long> hsetnx(String key, String field, String value) {
        return super.hsetnx(namespace.add(key), field, value);
    }

    @Override
    public Response<List<String>> hvals(byte[] key) {
        return super.hvals(namespace.add(key));
    }

    @Override
    public Response<List<String>> hvals(String key) {
        return super.hvals(namespace.add(key));
    }

    @Override
    public Response<Long> incr(byte[] key) {
        return super.incr(namespace.add(key));
    }

    @Override
    public Response<Long> incr(String key) {
        return super.incr(namespace.add(key));
    }

    @Override
    public Response<Long> incrBy(byte[] key, long integer) {
        return super.incrBy(namespace.add(key), integer);
    }

    @Override
    public Response<Long> incrBy(String key, long integer) {
        return super.incrBy(namespace.add(key), integer);
    }

    @Override
    public Response<Set<String>> keys(byte[] pattern) {
        return namespace.remove(super.keys(namespace.add(pattern)));
    }

    @Override
    public Response<Set<String>> keys(String pattern) {
        return namespace.remove(super.keys(namespace.add(pattern)));
    }

    @Override
    public Response<Long> lastsave() {
        return super.lastsave();
    }

    @Override
    public Response<String> lindex(byte[] key, int index) {
        return super.lindex(namespace.add(key), index);
    }

    @Override
    public Response<String> lindex(String key, int index) {
        return super.lindex(namespace.add(key), index);
    }

    @Override
    public Response<Long> linsert(byte[] key, LIST_POSITION where, byte[] pivot, byte[] value) {
        return super.linsert(namespace.add(key), where, pivot, value);
    }

    @Override
    public Response<Long> linsert(String key, LIST_POSITION where, String pivot, String value) {
        return super.linsert(namespace.add(key), where, pivot, value);
    }

    @Override
    public Response<Long> llen(byte[] key) {
        return super.llen(namespace.add(key));
    }

    @Override
    public Response<Long> llen(String key) {
        return super.llen(namespace.add(key));
    }

    @Override
    public Response<String> lpop(byte[] key) {
        return super.lpop(namespace.add(key));
    }

    @Override
    public Response<String> lpop(String key) {
        return super.lpop(namespace.add(key));
    }

    @Override
    public Response<Long> lpush(byte[] key, byte[] string) {
        return super.lpush(namespace.add(key), string);
    }

    @Override
    public Response<Long> lpush(String key, String string) {
        return super.lpush(namespace.add(key), string);
    }

    @Override
    public Response<Long> lpushx(byte[] key, byte[] bytes) {
        return super.lpushx(namespace.add(key), bytes);
    }

    @Override
    public Response<Long> lpushx(String key, String string) {
        return super.lpushx(namespace.add(key), string);
    }

    @Override
    public Response<List<String>> lrange(byte[] key, long start, long end) {
        return super.lrange(namespace.add(key), start, end);
    }

    @Override
    public Response<List<String>> lrange(String key, long start, long end) {
        return super.lrange(namespace.add(key), start, end);
    }

    @Override
    public Response<Long> lrem(byte[] key, long count, byte[] value) {
        return super.lrem(namespace.add(key), count, value);
    }

    @Override
    public Response<Long> lrem(String key, long count, String value) {
        return super.lrem(namespace.add(key), count, value);
    }

    @Override
    public Response<String> lset(byte[] key, long index, byte[] value) {
        return super.lset(namespace.add(key), index, value);
    }

    @Override
    public Response<String> lset(String key, long index, String value) {
        return super.lset(namespace.add(key), index, value);
    }

    @Override
    public Response<String> ltrim(byte[] key, long start, long end) {
        return super.ltrim(namespace.add(key), start, end);
    }

    @Override
    public Response<String> ltrim(String key, long start, long end) {
        return super.ltrim(namespace.add(key), start, end);
    }

    @Override
    public Response<List<String>> mget(byte[]... keys) {
        return super.mget(namespace.add(keys));
    }

    @Override
    public Response<List<String>> mget(String... keys) {
        return super.mget(namespace.add(keys));
    }

    @Override
    public Response<Long> move(byte[] key, int dbIndex) {
        return super.move(namespace.add(key), dbIndex);
    }

    @Override
    public Response<Long> move(String key, int dbIndex) {
        return super.move(namespace.add(key), dbIndex);
    }

    @Override
    public Response<String> mset(byte[]... keysvalues) {
        return super.mset(keysvalues);
    }

    @Override
    public Response<String> mset(String... keysvalues) {
        return super.mset(keysvalues);
    }

    @Override
    public Response<Long> msetnx(byte[]... keysvalues) {
        return super.msetnx(keysvalues);
    }

    @Override
    public Response<Long> msetnx(String... keysvalues) {
        return super.msetnx(keysvalues);
    }

    @Override
    public void multi() {
        super.multi();
    }

    @Override
    public Response<Long> persist(byte[] key) {
        return super.persist(namespace.add(key));
    }

    @Override
    public Response<Long> persist(String key) {
        return super.persist(namespace.add(key));
    }

    @Override
    public Response<Long> publish(byte[] channel, byte[] message) {
        return super.publish(channel, message);
    }

    @Override
    public Response<Long> publish(String channel, String message) {
        return super.publish(channel, message);
    }

    @Override
    public Response<String> rename(byte[] oldkey, byte[] newkey) {
        return super.rename(oldkey, newkey);
    }

    @Override
    public Response<String> rename(String oldkey, String newkey) {
        return super.rename(oldkey, newkey);
    }

    @Override
    public Response<Long> renamenx(byte[] oldkey, byte[] newkey) {
        return super.renamenx(oldkey, newkey);
    }

    @Override
    public Response<Long> renamenx(String oldkey, String newkey) {
        return super.renamenx(oldkey, newkey);
    }

    @Override
    public Response<String> rpop(byte[] key) {
        return super.rpop(namespace.add(key));
    }

    @Override
    public Response<String> rpop(String key) {
        return super.rpop(namespace.add(key));
    }

    @Override
    public Response<String> rpoplpush(byte[] srckey, byte[] dstkey) {
        return super.rpoplpush(srckey, namespace.add(dstkey));
    }

    @Override
    public Response<String> rpoplpush(String srckey, String dstkey) {
        return super.rpoplpush(srckey, namespace.add(dstkey));
    }

    @Override
    public Response<Long> rpush(byte[] key, byte[] string) {
        return super.rpush(namespace.add(key), string);
    }

    @Override
    public Response<Long> rpush(String key, String string) {
        return super.rpush(namespace.add(key), string);
    }

    @Override
    public Response<Long> rpushx(byte[] key, byte[] string) {
        return super.rpushx(namespace.add(key), string);
    }

    @Override
    public Response<Long> rpushx(String key, String string) {
        return super.rpushx(namespace.add(key), string);
    }

    @Override
    public Response<Long> sadd(byte[] key, byte[] member) {
        return super.sadd(namespace.add(key), member);
    }

    @Override
    public Response<Long> sadd(String key, String member) {
        return super.sadd(namespace.add(key), member);
    }

    @Override
    public Response<String> save() {
        return super.save();
    }

    @Override
    public Response<Long> scard(byte[] key) {
        return super.scard(namespace.add(key));
    }

    @Override
    public Response<Long> scard(String key) {
        return super.scard(namespace.add(key));
    }

    @Override
    public Response<Set<String>> sdiff(byte[]... keys) {
        return super.sdiff(namespace.add(keys));
    }

    @Override
    public Response<Set<String>> sdiff(String... keys) {
        return super.sdiff(namespace.add(keys));
    }

    @Override
    public Response<Long> sdiffstore(byte[] dstkey, byte[]... keys) {
        return super.sdiffstore(dstkey, keys);
    }

    @Override
    public Response<Long> sdiffstore(String dstkey, String... keys) {
        return super.sdiffstore(dstkey, keys);
    }

    @Override
    public Response<String> set(byte[] key, byte[] value) {
        return super.set(namespace.add(key), value);
    }

    @Override
    public Response<String> set(String key, String value) {
        return super.set(namespace.add(key), value);
    }

    @Override
    public Response<Boolean> setbit(String key, long offset, boolean value) {
        return super.setbit(namespace.add(key), offset, value);
    }

    @Override
    public Response<String> setex(byte[] key, int seconds, byte[] value) {
        return super.setex(namespace.add(key), seconds, value);
    }

    @Override
    public Response<String> setex(String key, int seconds, String value) {
        return super.setex(namespace.add(key), seconds, value);
    }

    @Override
    public Response<Long> setnx(byte[] key, byte[] value) {
        return super.setnx(namespace.add(key), value);
    }

    @Override
    public Response<Long> setnx(String key, String value) {
        return super.setnx(namespace.add(key), value);
    }

    @Override
    public Response<Long> setrange(String key, long offset, String value) {
        return super.setrange(namespace.add(key), offset, value);
    }

    @Override
    public Response<Set<String>> sinter(byte[]... keys) {
        return super.sinter(namespace.add(keys));
    }

    @Override
    public Response<Set<String>> sinter(String... keys) {
        return super.sinter(namespace.add(keys));
    }

    @Override
    public Response<Long> sinterstore(byte[] dstkey, byte[]... keys) {
        return super.sinterstore(dstkey, keys);
    }

    @Override
    public Response<Long> sinterstore(String dstkey, String... keys) {
        return super.sinterstore(dstkey, keys);
    }

    @Override
    public Response<Boolean> sismember(byte[] key, byte[] member) {
        return super.sismember(namespace.add(key), member);
    }

    @Override
    public Response<Boolean> sismember(String key, String member) {
        return super.sismember(namespace.add(key), member);
    }

    @Override
    public Response<Set<String>> smembers(byte[] key) {
        return super.smembers(namespace.add(key));
    }

    @Override
    public Response<Set<String>> smembers(String key) {
        return super.smembers(namespace.add(key));
    }

    @Override
    public Response<Long> smove(byte[] srckey, byte[] dstkey, byte[] member) {
        return super.smove(namespace.add(srckey), namespace.add(dstkey), member);
    }

    @Override
    public Response<Long> smove(String srckey, String dstkey, String member) {
        return super.smove(namespace.add(srckey), namespace.add(dstkey), member);
    }

    @Override
    public Response<Long> sort(byte[] key) {
        return super.sort(namespace.add(key));
    }

    @Override
    public Response<List<String>> sort(byte[] key, byte[] dstkey) {
        return super.sort(namespace.add(key), namespace.add(dstkey));
    }

    @Override
    public Response<List<String>> sort(byte[] key, SortingParams sortingParameters) {
        return super.sort(namespace.add(key), sortingParameters);
    }

    @Override
    public Response<List<String>> sort(byte[] key, SortingParams sortingParameters, byte[] dstkey) {
        return super.sort(namespace.add(key), sortingParameters, namespace.add(dstkey));
    }

    @Override
    public Response<Long> sort(String key) {
        return super.sort(namespace.add(key));
    }

    @Override
    public Response<List<String>> sort(String key, SortingParams sortingParameters) {
        return super.sort(namespace.add(key), sortingParameters);
    }

    @Override
    public Response<List<String>> sort(String key, SortingParams sortingParameters, String dstkey) {
        return super.sort(namespace.add(key), sortingParameters, namespace.add(dstkey));
    }

    @Override
    public Response<List<String>> sort(String key, String dstkey) {
        return super.sort(namespace.add(key), namespace.add(dstkey));
    }

    @Override
    public Response<String> spop(byte[] key) {
        return super.spop(namespace.add(key));
    }

    @Override
    public Response<String> spop(String key) {
        return super.spop(namespace.add(key));
    }

    @Override
    public Response<String> srandmember(byte[] key) {
        return super.srandmember(namespace.add(key));
    }

    @Override
    public Response<String> srandmember(String key) {
        return super.srandmember(namespace.add(key));
    }

    @Override
    public Response<Long> srem(byte[] key, byte[] member) {
        return super.srem(namespace.add(key), member);
    }

    @Override
    public Response<Long> srem(String key, String member) {
        return super.srem(namespace.add(key), member);
    }

    @Override
    public Response<Long> strlen(byte[] key) {
        return super.strlen(namespace.add(key));
    }

    @Override
    public Response<Long> strlen(String key) {
        return super.strlen(namespace.add(key));
    }

    @Override
    public Response<String> substr(byte[] key, int start, int end) {
        return super.substr(namespace.add(key), start, end);
    }

    @Override
    public Response<String> substr(String key, int start, int end) {
        return super.substr(namespace.add(key), start, end);
    }

    @Override
    public Response<Set<String>> sunion(byte[]... keys) {
        return super.sunion(namespace.add(keys));
    }

    @Override
    public Response<Set<String>> sunion(String... keys) {
        return super.sunion(namespace.add(keys));
    }

    @Override
    public Response<Long> sunionstore(byte[] dstkey, byte[]... keys) {
        return super.sunionstore(namespace.add(dstkey), namespace.add(keys));
    }

    @Override
    public Response<Long> sunionstore(String dstkey, String... keys) {
        return super.sunionstore(namespace.add(dstkey), namespace.add(keys));
    }

    @Override
    public Response<Long> ttl(byte[] key) {
        return super.ttl(namespace.add(key));
    }

    @Override
    public Response<Long> ttl(String key) {
        return super.ttl(namespace.add(key));
    }

    @Override
    public Response<String> type(byte[] key) {
        return super.type(namespace.add(key));
    }

    @Override
    public Response<String> type(String key) {
        return super.type(namespace.add(key));
    }

    @Override
    public Response<String> watch(byte[]... keys) {
        return super.watch(namespace.add(keys));
    }

    @Override
    public Response<String> watch(String... keys) {
        return super.watch(namespace.add(keys));
    }

    @Override
    public Response<Long> zadd(byte[] key, double score, byte[] member) {
        return super.zadd(namespace.add(key), score, member);
    }

    @Override
    public Response<Long> zadd(String key, double score, String member) {
        return super.zadd(namespace.add(key), score, member);
    }

    @Override
    public Response<Long> zcard(byte[] key) {
        return super.zcard(namespace.add(key));
    }

    @Override
    public Response<Long> zcard(String key) {
        return super.zcard(namespace.add(key));
    }

    @Override
    public Response<Long> zcount(byte[] key, double min, double max) {
        return super.zcount(namespace.add(key), min, max);
    }

    @Override
    public Response<Long> zcount(String key, double min, double max) {
        return super.zcount(namespace.add(key), min, max);
    }

    @Override
    public Response<Double> zincrby(byte[] key, double score, byte[] member) {
        return super.zincrby(namespace.add(key), score, member);
    }

    @Override
    public Response<Double> zincrby(String key, double score, String member) {
        return super.zincrby(namespace.add(key), score, member);
    }

    @Override
    public Response<Long> zinterstore(byte[] dstkey, byte[]... sets) {
        return super.zinterstore(namespace.add(dstkey), namespace.add(sets));
    }

    @Override
    public Response<Long> zinterstore(byte[] dstkey, ZParams params, byte[]... sets) {
        return super.zinterstore(namespace.add(dstkey), params, namespace.add(sets));
    }

    @Override
    public Response<Long> zinterstore(String dstkey, String... sets) {
        return super.zinterstore(namespace.add(dstkey), namespace.add(sets));
    }

    @Override
    public Response<Long> zinterstore(String dstkey, ZParams params, String... sets) {
        return super.zinterstore(namespace.add(dstkey), params, namespace.add(sets));
    }

    @Override
    public Response<Set<String>> zrange(byte[] key, int start, int end) {
        return super.zrange(namespace.add(key), start, end);
    }

    @Override
    public Response<Set<String>> zrange(String key, int start, int end) {
        return super.zrange(namespace.add(key), start, end);
    }

    @Override
    public Response<Set<String>> zrangeByScore(byte[] key, byte[] min, byte[] max) {
        return super.zrangeByScore(namespace.add(key), min, max);
    }

    @Override
    public Response<Set<String>> zrangeByScore(byte[] key, double min, double max) {
        return super.zrangeByScore(namespace.add(key), min, max);
    }

    @Override
    public Response<Set<String>> zrangeByScore(byte[] key, double min, double max, int offset, int count) {
        return super.zrangeByScore(namespace.add(key), min, max, offset, count);
    }

    @Override
    public Response<Set<String>> zrangeByScore(String key, double min, double max) {
        return super.zrangeByScore(namespace.add(key), min, max);
    }

    @Override
    public Response<Set<String>> zrangeByScore(String key, double min, double max, int offset, int count) {
        return super.zrangeByScore(namespace.add(key), min, max, offset, count);
    }

    @Override
    public Response<Set<String>> zrangeByScore(String key, String min, String max) {
        return super.zrangeByScore(namespace.add(key), min, max);
    }

    @Override
    public Response<Set<Tuple>> zrangeByScoreWithScores(byte[] key, double min, double max) {
        return super.zrangeByScoreWithScores(namespace.add(key), min, max);
    }

    @Override
    public Response<Set<Tuple>> zrangeByScoreWithScores(byte[] key, double min, double max, int offset, int count) {
        return super.zrangeByScoreWithScores(namespace.add(key), min, max, offset, count);
    }

    @Override
    public Response<Set<Tuple>> zrangeByScoreWithScores(String key, double min, double max) {
        return super.zrangeByScoreWithScores(namespace.add(key), min, max);
    }

    @Override
    public Response<Set<Tuple>> zrangeByScoreWithScores(String key, double min, double max, int offset, int count) {
        return super.zrangeByScoreWithScores(namespace.add(key), min, max, offset, count);
    }

    @Override
    public Response<Set<Tuple>> zrangeWithScores(byte[] key, int start, int end) {
        return super.zrangeWithScores(namespace.add(key), start, end);
    }

    @Override
    public Response<Set<Tuple>> zrangeWithScores(String key, int start, int end) {
        return super.zrangeWithScores(namespace.add(key), start, end);
    }

    @Override
    public Response<Long> zrank(byte[] key, byte[] member) {
        return super.zrank(namespace.add(key), member);
    }

    @Override
    public Response<Long> zrank(String key, String member) {
        return super.zrank(namespace.add(key), member);
    }

    @Override
    public Response<Long> zrem(byte[] key, byte[] member) {
        return super.zrem(namespace.add(key), member);
    }

    @Override
    public Response<Long> zrem(String key, String member) {
        return super.zrem(namespace.add(key), member);
    }

    @Override
    public Response<Long> zremrangeByRank(byte[] key, int start, int end) {
        return super.zremrangeByRank(namespace.add(key), start, end);
    }

    @Override
    public Response<Long> zremrangeByRank(String key, int start, int end) {
        return super.zremrangeByRank(namespace.add(key), start, end);
    }

    @Override
    public Response<Long> zremrangeByScore(byte[] key, double start, double end) {
        return super.zremrangeByScore(namespace.add(key), start, end);
    }

    @Override
    public Response<Long> zremrangeByScore(String key, double start, double end) {
        return super.zremrangeByScore(namespace.add(key), start, end);
    }

    @Override
    public Response<Set<String>> zrevrange(byte[] key, int start, int end) {
        return super.zrevrange(namespace.add(key), start, end);
    }

    @Override
    public Response<Set<String>> zrevrange(String key, int start, int end) {
        return super.zrevrange(namespace.add(key), start, end);
    }

    @Override
    public Response<Set<String>> zrevrangeByScore(byte[] key, byte[] max, byte[] min) {
        return super.zrevrangeByScore(namespace.add(key), max, min);
    }

    @Override
    public Response<Set<String>> zrevrangeByScore(byte[] key, double max, double min) {
        return super.zrevrangeByScore(namespace.add(key), max, min);
    }

    @Override
    public Response<Set<String>> zrevrangeByScore(byte[] key, double max, double min, int offset, int count) {
        return super.zrevrangeByScore(namespace.add(key), max, min, offset, count);
    }

    @Override
    public Response<Set<String>> zrevrangeByScore(String key, double max, double min) {
        return super.zrevrangeByScore(namespace.add(key), max, min);
    }

    @Override
    public Response<Set<String>> zrevrangeByScore(String key, double max, double min, int offset, int count) {
        return super.zrevrangeByScore(namespace.add(key), max, min, offset, count);
    }

    @Override
    public Response<Set<String>> zrevrangeByScore(String key, String max, String min) {
        return super.zrevrangeByScore(namespace.add(key), max, min);
    }

    @Override
    public Response<Set<Tuple>> zrevrangeByScoreWithScores(byte[] key, double max, double min) {
        return super.zrevrangeByScoreWithScores(namespace.add(key), max, min);
    }

    @Override
    public Response<Set<Tuple>> zrevrangeByScoreWithScores(byte[] key, double max, double min, int offset, int count) {
        return super.zrevrangeByScoreWithScores(namespace.add(key), max, min, offset, count);
    }

    @Override
    public Response<Set<Tuple>> zrevrangeByScoreWithScores(String key, double max, double min) {
        return super.zrevrangeByScoreWithScores(namespace.add(key), max, min);
    }

    @Override
    public Response<Set<Tuple>> zrevrangeByScoreWithScores(String key, double max, double min, int offset, int count) {
        return super.zrevrangeByScoreWithScores(namespace.add(key), max, min, offset, count);
    }

    @Override
    public Response<Set<Tuple>> zrevrangeWithScores(byte[] key, int start, int end) {
        return super.zrevrangeWithScores(namespace.add(key), start, end);
    }

    @Override
    public Response<Set<Tuple>> zrevrangeWithScores(String key, int start, int end) {
        return super.zrevrangeWithScores(namespace.add(key), start, end);
    }

    @Override
    public Response<Long> zrevrank(byte[] key, byte[] member) {
        return super.zrevrank(namespace.add(key), member);
    }

    @Override
    public Response<Long> zrevrank(String key, String member) {
        return super.zrevrank(namespace.add(key), member);
    }

    @Override
    public Response<Double> zscore(byte[] key, byte[] member) {
        return super.zscore(namespace.add(key), member);
    }

    @Override
    public Response<Double> zscore(String key, String member) {
        return super.zscore(namespace.add(key), member);
    }

    @Override
    public Response<Long> zunionstore(byte[] dstkey, byte[]... sets) {
        return super.zunionstore(namespace.add(dstkey), namespace.add(sets));
    }

    @Override
    public Response<Long> zunionstore(byte[] dstkey, ZParams params, byte[]... sets) {
        return super.zunionstore(namespace.add(dstkey), params, namespace.add(sets));
    }

    @Override
    public Response<Long> zunionstore(String dstkey, String... sets) {
        return super.zunionstore(namespace.add(dstkey), namespace.add(sets));
    }

    @Override
    public Response<Long> zunionstore(String dstkey, ZParams params, String... sets) {
        return super.zunionstore(namespace.add(dstkey), params, namespace.add(sets));
    }

}
