package com.trigonic.jedis;

import java.util.List;
import java.util.Map;
import java.util.Set;

import redis.clients.jedis.BinaryClient.LIST_POSITION;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.PipelineBlock;
import redis.clients.jedis.SortingParams;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.ZParams;

public class NamespaceJedis extends Jedis {
    private Namespace namespace;

    public NamespaceJedis(JedisShardInfo shardInfo) {
        super(shardInfo);
    }

    public NamespaceJedis(String host) {
        super(host);
    }

    public NamespaceJedis(String host, int port) {
        super(host, port);
    }

    public NamespaceJedis(String host, int port, int timeout) {
        super(host, port, timeout);
    }

    @Override
    public Long append(byte[] key, byte[] value) {
        return super.append(namespace.add(key), value);
    }

    @Override
    public Long append(String key, String value) {
        return super.append(namespace.add(key), value);
    }

    @Override
    public List<byte[]> blpop(int timeout, byte[]... keys) {
        return namespace.removeFirstBytes(super.blpop(timeout, namespace.add(keys)));
    }

    @Override
    public List<String> blpop(int timeout, String... keys) {
        return namespace.removeFirst(super.blpop(timeout, namespace.add(keys)));
    }

    @Override
    public List<byte[]> brpop(int timeout, byte[]... keys) {
        return namespace.removeFirstBytes(super.brpop(timeout, namespace.add(keys)));
    }

    @Override
    public List<String> brpop(int timeout, String... keys) {
        return namespace.removeFirst(super.brpop(timeout, namespace.add(keys)));
    }

    @Override
    public byte[] brpoplpush(byte[] source, byte[] destination, int timeout) {
        return super.brpoplpush(namespace.add(source), namespace.add(destination), timeout);
    }

    @Override
    public String brpoplpush(String source, String destination, int timeout) {
        return super.brpoplpush(namespace.add(source), namespace.add(destination), timeout);
    }

    @Override
    public Long decr(byte[] key) {
        return super.decr(namespace.add(key));
    }

    @Override
    public Long decr(String key) {
        return super.decr(namespace.add(key));
    }

    @Override
    public Long decrBy(byte[] key, long integer) {
        return super.decrBy(namespace.add(key), integer);
    }

    @Override
    public Long decrBy(String key, long integer) {
        return super.decrBy(namespace.add(key), integer);
    }

    @Override
    public Long del(byte[]... keys) {
        return super.del(namespace.add(keys));
    }

    @Override
    public Long del(String... keys) {
        return super.del(namespace.add(keys));
    }

    @Override
    public String echo(String string) {
        return super.echo(string);
    }

    @Override
    public Boolean exists(byte[] key) {
        return super.exists(namespace.add(key));
    }

    @Override
    public Boolean exists(String key) {
        return super.exists(namespace.add(key));
    }

    @Override
    public Long expire(byte[] key, int seconds) {
        return super.expire(namespace.add(key), seconds);
    }

    @Override
    public Long expire(String key, int seconds) {
        return super.expire(namespace.add(key), seconds);
    }

    @Override
    public Long expireAt(byte[] key, long unixTime) {
        return super.expireAt(namespace.add(key), unixTime);
    }

    @Override
    public Long expireAt(String key, long unixTime) {
        return super.expireAt(namespace.add(key), unixTime);
    }

    @Override
    public byte[] get(byte[] key) {
        return super.get(namespace.add(key));
    }

    @Override
    public String get(String key) {
        return super.get(namespace.add(key));
    }

    @Override
    public Long getbit(byte[] key, long offset) {
        return super.getbit(namespace.add(key), offset);
    }

    @Override
    public boolean getbit(String key, long offset) {
        return super.getbit(namespace.add(key), offset);
    }

    public String getNamespace() {
        return namespace.get();
    }

    @Override
    public String getrange(byte[] key, long startOffset, long endOffset) {
        return super.getrange(namespace.add(key), startOffset, endOffset);
    }

    @Override
    public String getrange(String key, long startOffset, long endOffset) {
        return super.getrange(namespace.add(key), startOffset, endOffset);
    }

    @Override
    public byte[] getSet(byte[] key, byte[] value) {
        return super.getSet(namespace.add(key), value);
    }

    @Override
    public String getSet(String key, String value) {
        return super.getSet(namespace.add(key), value);
    }

    @Override
    public Long hdel(byte[] key, byte[] field) {
        return super.hdel(namespace.add(key), field);
    }

    @Override
    public Long hdel(String key, String field) {
        return super.hdel(namespace.add(key), field);
    }

    @Override
    public Boolean hexists(byte[] key, byte[] field) {
        return super.hexists(namespace.add(key), field);
    }

    @Override
    public Boolean hexists(String key, String field) {
        return super.hexists(namespace.add(key), field);
    }

    @Override
    public byte[] hget(byte[] key, byte[] field) {
        return super.hget(namespace.add(key), field);
    }

    @Override
    public String hget(String key, String field) {
        return super.hget(namespace.add(key), field);
    }

    @Override
    public Map<byte[], byte[]> hgetAll(byte[] key) {
        return super.hgetAll(namespace.add(key));
    }

    @Override
    public Map<String, String> hgetAll(String key) {
        return super.hgetAll(namespace.add(key));
    }

    @Override
    public Long hincrBy(byte[] key, byte[] field, long value) {
        return super.hincrBy(namespace.add(key), field, value);
    }

    @Override
    public Long hincrBy(String key, String field, long value) {
        return super.hincrBy(namespace.add(key), field, value);
    }

    @Override
    public Set<byte[]> hkeys(byte[] key) {
        return super.hkeys(namespace.add(key));
    }

    @Override
    public Set<String> hkeys(String key) {
        return super.hkeys(namespace.add(key));
    }

    @Override
    public Long hlen(byte[] key) {
        return super.hlen(namespace.add(key));
    }

    @Override
    public Long hlen(String key) {
        return super.hlen(namespace.add(key));
    }

    @Override
    public List<byte[]> hmget(byte[] key, byte[]... fields) {
        return super.hmget(namespace.add(key), fields);
    }

    @Override
    public List<String> hmget(String key, String... fields) {
        return super.hmget(namespace.add(key), fields);
    }

    @Override
    public String hmset(byte[] key, Map<byte[], byte[]> hash) {
        return super.hmset(namespace.add(key), hash);
    }

    @Override
    public String hmset(String key, Map<String, String> hash) {
        return super.hmset(namespace.add(key), hash);
    }

    @Override
    public Long hset(byte[] key, byte[] field, byte[] value) {
        return super.hset(namespace.add(key), field, value);
    }

    @Override
    public Long hset(String key, String field, String value) {
        return super.hset(namespace.add(key), field, value);
    }

    @Override
    public Long hsetnx(byte[] key, byte[] field, byte[] value) {
        return super.hsetnx(namespace.add(key), field, value);
    }

    @Override
    public Long hsetnx(String key, String field, String value) {
        return super.hsetnx(namespace.add(key), field, value);
    }

    @Override
    public List<byte[]> hvals(byte[] key) {
        return super.hvals(namespace.add(key));
    }

    @Override
    public List<String> hvals(String key) {
        return super.hvals(namespace.add(key));
    }

    @Override
    public Long incr(byte[] key) {
        return super.incr(namespace.add(key));
    }

    @Override
    public Long incr(String key) {
        return super.incr(namespace.add(key));
    }

    @Override
    public Long incrBy(byte[] key, long integer) {
        return super.incrBy(namespace.add(key), integer);
    }

    @Override
    public Long incrBy(String key, long integer) {
        return super.incrBy(namespace.add(key), integer);
    }

    @Override
    public Set<byte[]> keys(byte[] pattern) {
        return super.keys(pattern);
    }

    @Override
    public Set<String> keys(String pattern) {
        return namespace.remove(super.keys(namespace.add(pattern)));
    }

    @Override
    public byte[] lindex(byte[] key, int index) {
        return super.lindex(namespace.add(key), index);
    }

    @Override
    public String lindex(String key, long index) {
        return super.lindex(namespace.add(key), index);
    }

    @Override
    public Long linsert(byte[] key, LIST_POSITION where, byte[] pivot, byte[] value) {
        return super.linsert(namespace.add(key), where, pivot, value);
    }

    @Override
    public Long linsert(String key, LIST_POSITION where, String pivot, String value) {
        return super.linsert(namespace.add(key), where, pivot, value);
    }

    @Override
    public Long llen(byte[] key) {
        return super.llen(namespace.add(key));
    }

    @Override
    public Long llen(String key) {
        return super.llen(namespace.add(key));
    }

    @Override
    public byte[] lpop(byte[] key) {
        return super.lpop(namespace.add(key));
    }

    @Override
    public String lpop(String key) {
        return super.lpop(namespace.add(key));
    }

    @Override
    public Long lpush(byte[] key, byte[] string) {
        return super.lpush(namespace.add(key), string);
    }

    @Override
    public Long lpush(String key, String string) {
        return super.lpush(namespace.add(key), string);
    }

    @Override
    public Long lpushx(byte[] key, byte[] string) {
        return super.lpushx(namespace.add(key), string);
    }

    @Override
    public Long lpushx(String key, String string) {
        return super.lpushx(namespace.add(key), string);
    }

    @Override
    public List<byte[]> lrange(byte[] key, int start, int end) {
        return super.lrange(namespace.add(key), start, end);
    }

    @Override
    public List<String> lrange(String key, long start, long end) {
        return super.lrange(namespace.add(key), start, end);
    }

    @Override
    public Long lrem(byte[] key, int count, byte[] value) {
        return super.lrem(namespace.add(key), count, value);
    }

    @Override
    public Long lrem(String key, long count, String value) {
        return super.lrem(namespace.add(key), count, value);
    }

    @Override
    public String lset(byte[] key, int index, byte[] value) {
        return super.lset(namespace.add(key), index, value);
    }

    @Override
    public String lset(String key, long index, String value) {
        return super.lset(namespace.add(key), index, value);
    }

    @Override
    public String ltrim(byte[] key, int start, int end) {
        return super.ltrim(namespace.add(key), start, end);
    }

    @Override
    public String ltrim(String key, long start, long end) {
        return super.ltrim(namespace.add(key), start, end);
    }

    @Override
    public List<byte[]> mget(byte[]... keys) {
        return super.mget(namespace.add(keys));
    }

    @Override
    public List<String> mget(String... keys) {
        return super.mget(namespace.add(keys));
    }

    @Override
    public Long move(byte[] key, int dbIndex) {
        return super.move(namespace.add(key), dbIndex);
    }

    @Override
    public Long move(String key, int dbIndex) {
        return super.move(namespace.add(key), dbIndex);
    }

    @Override
    public String mset(byte[]... keysvalues) {
        return super.mset(namespace.addEveryOther(keysvalues));
    }

    @Override
    public String mset(String... keysvalues) {
        return super.mset(namespace.addEveryOther(keysvalues));
    }

    @Override
    public Long msetnx(byte[]... keysvalues) {
        return super.msetnx(namespace.addEveryOther(keysvalues));
    }

    @Override
    public Long msetnx(String... keysvalues) {
        return super.msetnx(namespace.addEveryOther(keysvalues));
    }

    @Override
    public Long persist(byte[] key) {
        return super.persist(namespace.add(key));
    }

    @Override
    public Long persist(String key) {
        return super.persist(namespace.add(key));
    }

    @Override
    public Pipeline pipelined() {
        Pipeline pipeline = new NamespacePipeline(namespace);
        pipeline.setClient(client);
        return pipeline;
    }

    /**
     * Must use a {@link PipelineBlock} derived from {@link NamespacePipelineBlock} for namespaces to be honored here!
     */
    @Override
    public List<Object> pipelined(PipelineBlock jedisPipeline) {
        return super.pipelined(jedisPipeline);
    }

    @Override
    public void psubscribe(JedisPubSub jedisPubSub, String... patterns) {
        super.psubscribe(jedisPubSub, patterns);
    }

    @Override
    public Long publish(String channel, String message) {
        return super.publish(channel, message);
    }

    @Override
    public byte[] randomBinaryKey() {
        return super.randomBinaryKey();
    }

    /**
     * Namespaces are not honored here, since that would destroy the O(1) performance of the underlying method. This is
     * consistent with the Redis::Namespace implementation.
     */
    @Override
    public String randomKey() {
        return super.randomKey();
    }

    @Override
    public String rename(byte[] oldkey, byte[] newkey) {
        return super.rename(oldkey, newkey);
    }

    @Override
    public String rename(String oldkey, String newkey) {
        return super.rename(namespace.add(oldkey), namespace.add(newkey));
    }

    @Override
    public Long renamenx(byte[] oldkey, byte[] newkey) {
        return super.renamenx(oldkey, newkey);
    }

    @Override
    public Long renamenx(String oldkey, String newkey) {
        return super.renamenx(namespace.add(oldkey), namespace.add(newkey));
    }

    @Override
    public byte[] rpop(byte[] key) {
        return super.rpop(namespace.add(key));
    }

    @Override
    public String rpop(String key) {
        return super.rpop(namespace.add(key));
    }

    @Override
    public byte[] rpoplpush(byte[] srckey, byte[] dstkey) {
        return super.rpoplpush(srckey, namespace.add(dstkey));
    }

    @Override
    public String rpoplpush(String srckey, String dstkey) {
        return super.rpoplpush(srckey, namespace.add(dstkey));
    }

    @Override
    public Long rpush(byte[] key, byte[] string) {
        return super.rpush(namespace.add(key), string);
    }

    @Override
    public Long rpush(String key, String string) {
        return super.rpush(namespace.add(key), string);
    }

    @Override
    public Long rpushx(byte[] key, byte[] string) {
        return super.rpushx(namespace.add(key), string);
    }

    @Override
    public Long rpushx(String key, String string) {
        return super.rpushx(namespace.add(key), string);
    }

    @Override
    public Long sadd(byte[] key, byte[] member) {
        return super.sadd(namespace.add(key), member);
    }

    @Override
    public Long sadd(String key, String member) {
        return super.sadd(namespace.add(key), member);
    }

    @Override
    public Long scard(byte[] key) {
        return super.scard(namespace.add(key));
    }

    @Override
    public Long scard(String key) {
        return super.scard(namespace.add(key));
    }

    @Override
    public Set<byte[]> sdiff(byte[]... keys) {
        return super.sdiff(namespace.add(keys));
    }

    @Override
    public Set<String> sdiff(String... keys) {
        return super.sdiff(namespace.add(keys));
    }

    @Override
    public Long sdiffstore(byte[] dstkey, byte[]... keys) {
        return super.sdiffstore(namespace.add(dstkey), namespace.add(keys));
    }

    @Override
    public Long sdiffstore(String dstkey, String... keys) {
        return super.sdiffstore(namespace.add(dstkey), namespace.add(keys));
    }

    @Override
    public String set(byte[] key, byte[] value) {
        return super.set(namespace.add(key), value);
    }

    @Override
    public String set(String key, String value) {
        return super.set(namespace.add(key), value);
    }

    @Override
    public Long setbit(byte[] key, long offset, byte[] value) {
        return super.setbit(namespace.add(key), offset, value);
    }

    @Override
    public boolean setbit(String key, long offset, boolean value) {
        return super.setbit(namespace.add(key), offset, value);
    }

    @Override
    public String setex(byte[] key, int seconds, byte[] value) {
        return super.setex(namespace.add(key), seconds, value);
    }

    @Override
    public String setex(String key, int seconds, String value) {
        return super.setex(namespace.add(key), seconds, value);
    }

    public void setNamespace(String namespace) {
        this.namespace = new Namespace(namespace);
    }

    @Override
    public Long setnx(byte[] key, byte[] value) {
        return super.setnx(namespace.add(key), value);
    }

    @Override
    public Long setnx(String key, String value) {
        return super.setnx(namespace.add(key), value);
    }

    @Override
    public long setrange(byte[] key, long offset, byte[] value) {
        return super.setrange(namespace.add(key), offset, value);
    }

    @Override
    public long setrange(String key, long offset, String value) {
        return super.setrange(namespace.add(key), offset, value);
    }

    @Override
    public Set<byte[]> sinter(byte[]... keys) {
        return super.sinter(namespace.add(keys));
    }

    @Override
    public Set<String> sinter(String... keys) {
        return super.sinter(namespace.add(keys));
    }

    @Override
    public Long sinterstore(byte[] dstkey, byte[]... keys) {
        return super.sinterstore(namespace.add(dstkey), namespace.add(keys));
    }

    @Override
    public Long sinterstore(String dstkey, String... keys) {
        return super.sinterstore(namespace.add(dstkey), namespace.add(keys));
    }

    @Override
    public Boolean sismember(byte[] key, byte[] member) {
        return super.sismember(namespace.add(key), member);
    }

    @Override
    public Boolean sismember(String key, String member) {
        return super.sismember(namespace.add(key), member);
    }

    @Override
    public Set<byte[]> smembers(byte[] key) {
        return super.smembers(namespace.add(key));
    }

    @Override
    public Set<String> smembers(String key) {
        return super.smembers(namespace.add(key));
    }

    @Override
    public Long smove(byte[] srckey, byte[] dstkey, byte[] member) {
        return super.smove(namespace.add(srckey), namespace.add(dstkey), member);
    }

    @Override
    public Long smove(String srckey, String dstkey, String member) {
        return super.smove(namespace.add(srckey), namespace.add(dstkey), member);
    }

    @Override
    public List<byte[]> sort(byte[] key) {
        return super.sort(namespace.add(key));
    }

    @Override
    public Long sort(byte[] key, byte[] dstkey) {
        return super.sort(namespace.add(key), namespace.add(dstkey));
    }

    @Override
    public List<byte[]> sort(byte[] key, SortingParams sortingParameters) {
        return super.sort(namespace.add(key), namespace.add(sortingParameters));
    }

    @Override
    public Long sort(byte[] key, SortingParams sortingParameters, byte[] dstkey) {
        return super.sort(namespace.add(key), namespace.add(sortingParameters), namespace.add(dstkey));
    }

    @Override
    public List<String> sort(String key) {
        return super.sort(namespace.add(key));
    }

    @Override
    public List<String> sort(String key, SortingParams sortingParameters) {
        return super.sort(namespace.add(key), namespace.add(sortingParameters));
    }

    @Override
    public Long sort(String key, SortingParams sortingParameters, String dstkey) {
        return super.sort(namespace.add(key), namespace.add(sortingParameters), namespace.add(dstkey));
    }

    @Override
    public Long sort(String key, String dstkey) {
        return super.sort(namespace.add(key), namespace.add(dstkey));
    }

    @Override
    public byte[] spop(byte[] key) {
        return super.spop(namespace.add(key));
    }

    @Override
    public String spop(String key) {
        return super.spop(namespace.add(key));
    }

    @Override
    public byte[] srandmember(byte[] key) {
        return super.srandmember(namespace.add(key));
    }

    @Override
    public String srandmember(String key) {
        return super.srandmember(namespace.add(key));
    }

    @Override
    public Long srem(byte[] key, byte[] member) {
        return super.srem(namespace.add(key), member);
    }

    @Override
    public Long srem(String key, String member) {
        return super.srem(namespace.add(key), member);
    }

    @Override
    public Long strlen(byte[] key) {
        return super.strlen(namespace.add(key));
    }

    @Override
    public Long strlen(String key) {
        return super.strlen(namespace.add(key));
    }

    @Override
    public void subscribe(JedisPubSub jedisPubSub, String... channels) {
        super.subscribe(jedisPubSub, channels);
    }

    @Override
    public byte[] substr(byte[] key, int start, int end) {
        return super.substr(namespace.add(key), start, end);
    }

    @Override
    public String substr(String key, int start, int end) {
        return super.substr(namespace.add(key), start, end);
    }

    @Override
    public Set<byte[]> sunion(byte[]... keys) {
        return super.sunion(namespace.add(keys));
    }

    @Override
    public Set<String> sunion(String... keys) {
        return super.sunion(namespace.add(keys));
    }

    @Override
    public Long sunionstore(byte[] dstkey, byte[]... keys) {
        return super.sunionstore(namespace.add(dstkey), namespace.add(keys));
    }

    @Override
    public Long sunionstore(String dstkey, String... keys) {
        return super.sunionstore(namespace.add(dstkey), namespace.add(keys));
    }

    @Override
    public Long ttl(byte[] key) {
        return super.ttl(namespace.add(key));
    }

    @Override
    public Long ttl(String key) {
        return super.ttl(namespace.add(key));
    }

    @Override
    public String type(byte[] key) {
        return super.type(namespace.add(key));
    }

    @Override
    public String type(String key) {
        return super.type(namespace.add(key));
    }

    @Override
    public String watch(byte[]... keys) {
        return super.watch(namespace.add(keys));
    }

    @Override
    public String watch(String... keys) {
        return super.watch(namespace.add(keys));
    }

    public NamespaceJedis withNamespace(String ns) {
        setNamespace(ns);
        return this;
    }

    @Override
    public Long zadd(byte[] key, double score, byte[] member) {
        return super.zadd(namespace.add(key), score, member);
    }

    @Override
    public Long zadd(String key, double score, String member) {
        return super.zadd(namespace.add(key), score, member);
    }

    @Override
    public Long zcard(byte[] key) {
        return super.zcard(namespace.add(key));
    }

    @Override
    public Long zcard(String key) {
        return super.zcard(namespace.add(key));
    }

    @Override
    public Long zcount(byte[] key, double min, double max) {
        return super.zcount(namespace.add(key), min, max);
    }

    @Override
    public Long zcount(String key, double min, double max) {
        return super.zcount(namespace.add(key), min, max);
    }

    @Override
    public Double zincrby(byte[] key, double score, byte[] member) {
        return super.zincrby(namespace.add(key), score, member);
    }

    @Override
    public Double zincrby(String key, double score, String member) {
        return super.zincrby(namespace.add(key), score, member);
    }

    @Override
    public Long zinterstore(byte[] dstkey, byte[]... sets) {
        return super.zinterstore(namespace.add(dstkey), namespace.add(sets));
    }

    @Override
    public Long zinterstore(byte[] dstkey, ZParams params, byte[]... sets) {
        return super.zinterstore(namespace.add(dstkey), params, namespace.add(sets));
    }

    @Override
    public Long zinterstore(String dstkey, String... sets) {
        return super.zinterstore(namespace.add(dstkey), namespace.add(sets));
    }

    @Override
    public Long zinterstore(String dstkey, ZParams params, String... sets) {
        return super.zinterstore(namespace.add(dstkey), params, namespace.add(sets));
    }

    @Override
    public Set<byte[]> zrange(byte[] key, int start, int end) {
        return super.zrange(namespace.add(key), start, end);
    }

    @Override
    public Set<String> zrange(String key, int start, int end) {
        return super.zrange(namespace.add(key), start, end);
    }

    @Override
    public Set<byte[]> zrangeByScore(byte[] key, byte[] min, byte[] max) {
        return super.zrangeByScore(namespace.add(key), min, max);
    }

    @Override
    public Set<byte[]> zrangeByScore(byte[] key, double min, double max) {
        return super.zrangeByScore(namespace.add(key), min, max);
    }

    @Override
    public Set<byte[]> zrangeByScore(byte[] key, double min, double max, int offset, int count) {
        return super.zrangeByScore(namespace.add(key), min, max, offset, count);
    }

    @Override
    public Set<String> zrangeByScore(String key, double min, double max) {
        return super.zrangeByScore(namespace.add(key), min, max);
    }

    @Override
    public Set<String> zrangeByScore(String key, double min, double max, int offset, int count) {
        return super.zrangeByScore(namespace.add(key), min, max, offset, count);
    }

    @Override
    public Set<String> zrangeByScore(String key, String min, String max) {
        return super.zrangeByScore(namespace.add(key), min, max);
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(byte[] key, double min, double max) {
        return super.zrangeByScoreWithScores(namespace.add(key), min, max);
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(byte[] key, double min, double max, int offset, int count) {
        return super.zrangeByScoreWithScores(namespace.add(key), min, max, offset, count);
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(String key, double min, double max) {
        return super.zrangeByScoreWithScores(namespace.add(key), min, max);
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(String key, double min, double max, int offset, int count) {
        return super.zrangeByScoreWithScores(namespace.add(key), min, max, offset, count);
    }

    @Override
    public Set<Tuple> zrangeWithScores(byte[] key, int start, int end) {
        return super.zrangeWithScores(namespace.add(key), start, end);
    }

    @Override
    public Set<Tuple> zrangeWithScores(String key, int start, int end) {
        return super.zrangeWithScores(namespace.add(key), start, end);
    }

    @Override
    public Long zrank(byte[] key, byte[] member) {
        return super.zrank(namespace.add(key), member);
    }

    @Override
    public Long zrank(String key, String member) {
        return super.zrank(namespace.add(key), member);
    }

    @Override
    public Long zrem(byte[] key, byte[] member) {
        return super.zrem(namespace.add(key), member);
    }

    @Override
    public Long zrem(String key, String member) {
        return super.zrem(namespace.add(key), member);
    }

    @Override
    public Long zremrangeByRank(byte[] key, int start, int end) {
        return super.zremrangeByRank(namespace.add(key), start, end);
    }

    @Override
    public Long zremrangeByRank(String key, int start, int end) {
        return super.zremrangeByRank(namespace.add(key), start, end);
    }

    @Override
    public Long zremrangeByScore(byte[] key, double start, double end) {
        return super.zremrangeByScore(namespace.add(key), start, end);
    }

    @Override
    public Long zremrangeByScore(String key, double start, double end) {
        return super.zremrangeByScore(namespace.add(key), start, end);
    }

    @Override
    public Set<byte[]> zrevrange(byte[] key, int start, int end) {
        return super.zrevrange(namespace.add(key), start, end);
    }

    @Override
    public Set<String> zrevrange(String key, int start, int end) {
        return super.zrevrange(namespace.add(key), start, end);
    }

    @Override
    public Set<byte[]> zrevrangeByScore(byte[] key, byte[] max, byte[] min) {
        return super.zrevrangeByScore(namespace.add(key), max, min);
    }

    @Override
    public Set<byte[]> zrevrangeByScore(byte[] key, double max, double min) {
        return super.zrevrangeByScore(namespace.add(key), max, min);
    }

    @Override
    public Set<byte[]> zrevrangeByScore(byte[] key, double max, double min, int offset, int count) {
        return super.zrevrangeByScore(namespace.add(key), max, min, offset, count);
    }

    @Override
    public Set<String> zrevrangeByScore(String key, double max, double min) {
        return super.zrevrangeByScore(namespace.add(key), max, min);
    }

    @Override
    public Set<String> zrevrangeByScore(String key, double max, double min, int offset, int count) {
        return super.zrevrangeByScore(namespace.add(key), max, min, offset, count);
    }

    @Override
    public Set<String> zrevrangeByScore(String key, String max, String min) {
        return super.zrevrangeByScore(namespace.add(key), max, min);
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(byte[] key, double max, double min) {
        return super.zrevrangeByScoreWithScores(namespace.add(key), max, min);
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(byte[] key, double max, double min, int offset, int count) {
        return super.zrevrangeByScoreWithScores(namespace.add(key), max, min, offset, count);
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(String key, double max, double min) {
        return super.zrevrangeByScoreWithScores(namespace.add(key), max, min);
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(String key, double max, double min, int offset, int count) {
        return super.zrevrangeByScoreWithScores(namespace.add(key), max, min, offset, count);
    }

    @Override
    public Set<Tuple> zrevrangeWithScores(byte[] key, int start, int end) {
        return super.zrevrangeWithScores(namespace.add(key), start, end);
    }

    @Override
    public Set<Tuple> zrevrangeWithScores(String key, int start, int end) {
        return super.zrevrangeWithScores(namespace.add(key), start, end);
    }

    @Override
    public Long zrevrank(byte[] key, byte[] member) {
        return super.zrevrank(namespace.add(key), member);
    }

    @Override
    public Long zrevrank(String key, String member) {
        return super.zrevrank(namespace.add(key), member);
    }

    @Override
    public Double zscore(byte[] key, byte[] member) {
        return super.zscore(namespace.add(key), member);
    }

    @Override
    public Double zscore(String key, String member) {
        return super.zscore(namespace.add(key), member);
    }

    @Override
    public Long zunionstore(byte[] dstkey, byte[]... sets) {
        return super.zunionstore(namespace.add(dstkey), namespace.add(sets));
    }

    @Override
    public Long zunionstore(byte[] dstkey, ZParams params, byte[]... sets) {
        return super.zunionstore(namespace.add(dstkey), params, namespace.add(sets));
    }

    @Override
    public Long zunionstore(String dstkey, String... sets) {
        return super.zunionstore(namespace.add(dstkey), namespace.add(sets));
    }

    @Override
    public Long zunionstore(String dstkey, ZParams params, String... sets) {
        return super.zunionstore(namespace.add(dstkey), params, namespace.add(sets));
    }
}
