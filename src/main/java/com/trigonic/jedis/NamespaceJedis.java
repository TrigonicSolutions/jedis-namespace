package com.trigonic.jedis;

import java.util.List;
import java.util.Map;
import java.util.Set;

import redis.clients.jedis.BinaryClient.LIST_POSITION;
import redis.clients.jedis.BinaryJedisPubSub;
import redis.clients.jedis.Client;
import redis.clients.jedis.DebugParams;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisMonitor;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.PipelineBlock;
import redis.clients.jedis.SortingParams;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.TransactionBlock;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.ZParams;

public class NamespaceJedis extends Jedis {
    private NamespaceHandler namespace;
    private Jedis wrapped;

    public NamespaceJedis(String namespace, Jedis jedis) {
        super((String) null);
        this.namespace = new NamespaceHandler(namespace);
        this.wrapped = jedis;
    }

    @Override
    public Long append(byte[] key, byte[] value) {
        return wrapped.append(namespace.add(key), value);
    }
    
    @Override
    public Long append(String key, String value) {
        return wrapped.append(namespace.add(key), value);
    }
    
    @Override
    public String auth(String password) {
        return wrapped.auth(password);
    }
    
    @Override
    public String bgrewriteaof() {
        return wrapped.bgrewriteaof();
    }
    
    @Override
    public String bgsave() {
        return wrapped.bgsave();
    }
    
    @Override
    public List<byte[]> blpop(int timeout, byte[]... keys) {
        return namespace.removeFirstBytes(wrapped.blpop(timeout, namespace.add(keys)));
    }
    
    @Override
    public List<String> blpop(int timeout, String... keys) {
        return namespace.removeFirst(wrapped.blpop(timeout, namespace.add(keys)));
    }
    
    @Override
    public List<byte[]> brpop(int timeout, byte[]... keys) {
        return namespace.removeFirstBytes(wrapped.brpop(timeout, namespace.add(keys)));
    }
    
    @Override
    public List<String> brpop(int timeout, String... keys) {
        return namespace.removeFirst(wrapped.brpop(timeout, namespace.add(keys)));
    }
    
    @Override
    public byte[] brpoplpush(byte[] source, byte[] destination, int timeout) {
        return wrapped.brpoplpush(namespace.add(source), namespace.add(destination), timeout);
    }
    
    @Override
    public String brpoplpush(String source, String destination, int timeout) {
        return wrapped.brpoplpush(namespace.add(source), namespace.add(destination), timeout);
    }
    
    @Override
    public List<String> configGet(String pattern) {
        return wrapped.configGet(pattern);
    }
    
    @Override
    public String configResetStat() {
        return wrapped.configResetStat();
    }
    
    @Override
    public String configSet(String parameter, String value) {
        return wrapped.configSet(parameter, value);
    }
    
    @Override
    public void connect() {
        wrapped.connect();
    }
    
    @Override
    public Long dbSize() {
        return wrapped.dbSize();
    }
    
    @Override
    public String debug(DebugParams params) {
        return wrapped.debug(params);
    }
    
    @Override
    public Long decr(byte[] key) {
        return wrapped.decr(namespace.add(key));
    }
    
    @Override
    public Long decr(String key) {
        return wrapped.decr(namespace.add(key));
    }
    
    @Override
    public Long decrBy(byte[] key, long integer) {
        return wrapped.decrBy(namespace.add(key), integer);
    }
    
    @Override
    public Long decrBy(String key, long integer) {
        return wrapped.decrBy(namespace.add(key), integer);
    }
    
    @Override
    public Long del(byte[]... keys) {
        return wrapped.del(namespace.add(keys));
    }
    
    @Override
    public Long del(String... keys) {
        return wrapped.del(namespace.add(keys));
    }
    
    @Override
    public void disconnect() {
        wrapped.disconnect();
    }
    
    @Override
    public byte[] echo(byte[] string) {
        return wrapped.echo(string);
    }
    
    @Override
    public String echo(String string) {
        return wrapped.echo(string);
    }
    
    @Override
    public Boolean exists(byte[] key) {
        return wrapped.exists(namespace.add(key));
    }
    
    @Override
    public Boolean exists(String key) {
        return wrapped.exists(namespace.add(key));
    }
    
    @Override
    public Long expire(byte[] key, int seconds) {
        return wrapped.expire(namespace.add(key), seconds);
    }
    
    @Override
    public Long expire(String key, int seconds) {
        return wrapped.expire(namespace.add(key), seconds);
    }
    
    @Override
    public Long expireAt(byte[] key, long unixTime) {
        return wrapped.expireAt(namespace.add(key), unixTime);
    }
    
    @Override
    public Long expireAt(String key, long unixTime) {
        return wrapped.expireAt(namespace.add(key), unixTime);
    }
    
    @Override
    public String flushAll() {
        return wrapped.flushAll();
    }
    
    @Override
    public String flushDB() {
        return wrapped.flushDB();
    }

    @Override
    public byte[] get(byte[] key) {
        return wrapped.get(namespace.add(key));
    }

    @Override
    public String get(String key) {
        return wrapped.get(namespace.add(key));
    }

    @Override
    public Boolean getbit(byte[] key, long offset) {
        return wrapped.getbit(namespace.add(key), offset);
    }

    @Override
    public Boolean getbit(String key, long offset) {
        return wrapped.getbit(namespace.add(key), offset);
    }

    @Override
    public Client getClient() {
        return wrapped.getClient();
    }

    @Override
    public Long getDB() {
        return wrapped.getDB();
    }

    public String getNamespace() {
        return namespace.get();
    }

    @Override
    public String getrange(byte[] key, long startOffset, long endOffset) {
        return wrapped.getrange(namespace.add(key), startOffset, endOffset);
    }

    @Override
    public String getrange(String key, long startOffset, long endOffset) {
        return wrapped.getrange(namespace.add(key), startOffset, endOffset);
    }

    @Override
    public byte[] getSet(byte[] key, byte[] value) {
        return wrapped.getSet(namespace.add(key), value);
    }

    @Override
    public String getSet(String key, String value) {
        return wrapped.getSet(namespace.add(key), value);
    }

    @Override
    public Long hdel(byte[] key, byte[]... fields) {
        return wrapped.hdel(namespace.add(key), fields);
    }

    @Override
    public Long hdel(String key, String... fields) {
        return wrapped.hdel(namespace.add(key), fields);
    }

    @Override
    public Boolean hexists(byte[] key, byte[] field) {
        return wrapped.hexists(namespace.add(key), field);
    }

    @Override
    public Boolean hexists(String key, String field) {
        return wrapped.hexists(namespace.add(key), field);
    }

    @Override
    public byte[] hget(byte[] key, byte[] field) {
        return wrapped.hget(namespace.add(key), field);
    }

    @Override
    public String hget(String key, String field) {
        return wrapped.hget(namespace.add(key), field);
    }

    @Override
    public Map<byte[], byte[]> hgetAll(byte[] key) {
        return wrapped.hgetAll(namespace.add(key));
    }

    @Override
    public Map<String, String> hgetAll(String key) {
        return wrapped.hgetAll(namespace.add(key));
    }

    @Override
    public Long hincrBy(byte[] key, byte[] field, long value) {
        return wrapped.hincrBy(namespace.add(key), field, value);
    }

    @Override
    public Long hincrBy(String key, String field, long value) {
        return wrapped.hincrBy(namespace.add(key), field, value);
    }

    @Override
    public Set<byte[]> hkeys(byte[] key) {
        return wrapped.hkeys(namespace.add(key));
    }

    @Override
    public Set<String> hkeys(String key) {
        return wrapped.hkeys(namespace.add(key));
    }

    @Override
    public Long hlen(byte[] key) {
        return wrapped.hlen(namespace.add(key));
    }

    @Override
    public Long hlen(String key) {
        return wrapped.hlen(namespace.add(key));
    }

    @Override
    public List<byte[]> hmget(byte[] key, byte[]... fields) {
        return wrapped.hmget(namespace.add(key), fields);
    }

    @Override
    public List<String> hmget(String key, String... fields) {
        return wrapped.hmget(namespace.add(key), fields);
    }

    @Override
    public String hmset(byte[] key, Map<byte[], byte[]> hash) {
        return wrapped.hmset(namespace.add(key), hash);
    }

    @Override
    public String hmset(String key, Map<String, String> hash) {
        return wrapped.hmset(namespace.add(key), hash);
    }

    @Override
    public Long hset(byte[] key, byte[] field, byte[] value) {
        return wrapped.hset(namespace.add(key), field, value);
    }

    @Override
    public Long hset(String key, String field, String value) {
        return wrapped.hset(namespace.add(key), field, value);
    }

    @Override
    public Long hsetnx(byte[] key, byte[] field, byte[] value) {
        return wrapped.hsetnx(namespace.add(key), field, value);
    }

    @Override
    public Long hsetnx(String key, String field, String value) {
        return wrapped.hsetnx(namespace.add(key), field, value);
    }

    @Override
    public List<byte[]> hvals(byte[] key) {
        return wrapped.hvals(namespace.add(key));
    }

    @Override
    public List<String> hvals(String key) {
        return wrapped.hvals(namespace.add(key));
    }

    @Override
    public Long incr(byte[] key) {
        return wrapped.incr(namespace.add(key));
    }

    @Override
    public Long incr(String key) {
        return wrapped.incr(namespace.add(key));
    }

    @Override
    public Long incrBy(byte[] key, long integer) {
        return wrapped.incrBy(namespace.add(key), integer);
    }

    @Override
    public Long incrBy(String key, long integer) {
        return wrapped.incrBy(namespace.add(key), integer);
    }

    @Override
    public String info() {
        return wrapped.info();
    }

    @Override
    public boolean isConnected() {
        return wrapped.isConnected();
    }

    @Override
    public Set<byte[]> keys(byte[] pattern) {
        return wrapped.keys(pattern);
    }

    @Override
    public Set<String> keys(String pattern) {
        return namespace.remove(wrapped.keys(namespace.add(pattern)));
    }

    @Override
    public Long lastsave() {
        return wrapped.lastsave();
    }

    @Override
    public byte[] lindex(byte[] key, int index) {
        return wrapped.lindex(namespace.add(key), index);
    }

    @Override
    public String lindex(String key, long index) {
        return wrapped.lindex(namespace.add(key), index);
    }

    @Override
    public Long linsert(byte[] key, LIST_POSITION where, byte[] pivot, byte[] value) {
        return wrapped.linsert(namespace.add(key), where, pivot, value);
    }

    @Override
    public Long linsert(String key, LIST_POSITION where, String pivot, String value) {
        return wrapped.linsert(namespace.add(key), where, pivot, value);
    }

    @Override
    public Long llen(byte[] key) {
        return wrapped.llen(namespace.add(key));
    }

    @Override
    public Long llen(String key) {
        return wrapped.llen(namespace.add(key));
    }

    @Override
    public byte[] lpop(byte[] key) {
        return wrapped.lpop(namespace.add(key));
    }

    @Override
    public String lpop(String key) {
        return wrapped.lpop(namespace.add(key));
    }

    @Override
    public Long lpush(byte[] key, byte[]... strings) {
        return wrapped.lpush(namespace.add(key), strings);
    }

    @Override
    public Long lpush(String key, String... strings) {
        return wrapped.lpush(namespace.add(key), strings);
    }

    @Override
    public Long lpushx(byte[] key, byte[] string) {
        return wrapped.lpushx(namespace.add(key), string);
    }

    @Override
    public Long lpushx(String key, String string) {
        return wrapped.lpushx(namespace.add(key), string);
    }

    @Override
    public List<byte[]> lrange(byte[] key, int start, int end) {
        return wrapped.lrange(namespace.add(key), start, end);
    }

    @Override
    public List<String> lrange(String key, long start, long end) {
        return wrapped.lrange(namespace.add(key), start, end);
    }

    @Override
    public Long lrem(byte[] key, int count, byte[] value) {
        return wrapped.lrem(namespace.add(key), count, value);
    }

    @Override
    public Long lrem(String key, long count, String value) {
        return wrapped.lrem(namespace.add(key), count, value);
    }

    @Override
    public String lset(byte[] key, int index, byte[] value) {
        return wrapped.lset(namespace.add(key), index, value);
    }

    @Override
    public String lset(String key, long index, String value) {
        return wrapped.lset(namespace.add(key), index, value);
    }

    @Override
    public String ltrim(byte[] key, int start, int end) {
        return wrapped.ltrim(namespace.add(key), start, end);
    }

    @Override
    public String ltrim(String key, long start, long end) {
        return wrapped.ltrim(namespace.add(key), start, end);
    }

    @Override
    public List<byte[]> mget(byte[]... keys) {
        return wrapped.mget(namespace.add(keys));
    }

    @Override
    public List<String> mget(String... keys) {
        return wrapped.mget(namespace.add(keys));
    }

    @Override
    public void monitor(JedisMonitor jedisMonitor) {
        wrapped.monitor(jedisMonitor);
    }

    @Override
    public Long move(byte[] key, int dbIndex) {
        return wrapped.move(namespace.add(key), dbIndex);
    }

    @Override
    public Long move(String key, int dbIndex) {
        return wrapped.move(namespace.add(key), dbIndex);
    }

    @Override
    public String mset(byte[]... keysvalues) {
        return wrapped.mset(namespace.addEveryOther(keysvalues));
    }

    @Override
    public String mset(String... keysvalues) {
        return wrapped.mset(namespace.addEveryOther(keysvalues));
    }

    @Override
    public Long msetnx(byte[]... keysvalues) {
        return wrapped.msetnx(namespace.addEveryOther(keysvalues));
    }

    @Override
    public Long msetnx(String... keysvalues) {
        return wrapped.msetnx(namespace.addEveryOther(keysvalues));
    }

    @Override
    public Transaction multi() {
        return wrapped.multi();
    }

    @Override
    public List<Object> multi(TransactionBlock jedisTransaction) {
        return wrapped.multi(jedisTransaction);
    }

    @Override
    public Long persist(byte[] key) {
        return wrapped.persist(namespace.add(key));
    }

    @Override
    public Long persist(String key) {
        return wrapped.persist(namespace.add(key));
    }

    @Override
    public String ping() {
        return wrapped.ping();
    }

    @Override
    public Pipeline pipelined() {
        Pipeline pipeline = new NamespacePipeline(namespace);
        pipeline.setClient(getClient());
        return pipeline;
    }

    /**
     * Must use a {@link PipelineBlock} derived from {@link NamespacePipelineBlock} for namespaces to be honored here!
     */
    @Override
    public List<Object> pipelined(PipelineBlock jedisPipeline) {
        return wrapped.pipelined(jedisPipeline);
    }

    @Override
    public void psubscribe(BinaryJedisPubSub jedisPubSub, byte[]... patterns) {
        wrapped.psubscribe(jedisPubSub, patterns);
    }

    @Override
    public void psubscribe(JedisPubSub jedisPubSub, String... patterns) {
        wrapped.psubscribe(jedisPubSub, patterns);
    }

    @Override
    public Long publish(byte[] channel, byte[] message) {
        return wrapped.publish(channel, message);
    }

    @Override
    public Long publish(String channel, String message) {
        return wrapped.publish(channel, message);
    }

    @Override
    public String quit() {
        return wrapped.quit();
    }

    @Override
    public byte[] randomBinaryKey() {
        return wrapped.randomBinaryKey();
    }

    /**
     * Namespaces are not honored here, since that would destroy the O(1) performance of the underlying method. This is
     * consistent with the Redis::Namespace implementation.
     */
    @Override
    public String randomKey() {
        return wrapped.randomKey();
    }

    @Override
    public String rename(byte[] oldkey, byte[] newkey) {
        return wrapped.rename(oldkey, newkey);
    }

    @Override
    public String rename(String oldkey, String newkey) {
        return wrapped.rename(namespace.add(oldkey), namespace.add(newkey));
    }

    @Override
    public Long renamenx(byte[] oldkey, byte[] newkey) {
        return wrapped.renamenx(oldkey, newkey);
    }

    @Override
    public Long renamenx(String oldkey, String newkey) {
        return wrapped.renamenx(namespace.add(oldkey), namespace.add(newkey));
    }

    @Override
    public byte[] rpop(byte[] key) {
        return wrapped.rpop(namespace.add(key));
    }

    @Override
    public String rpop(String key) {
        return wrapped.rpop(namespace.add(key));
    }

    @Override
    public byte[] rpoplpush(byte[] srckey, byte[] dstkey) {
        return wrapped.rpoplpush(srckey, namespace.add(dstkey));
    }

    @Override
    public String rpoplpush(String srckey, String dstkey) {
        return wrapped.rpoplpush(srckey, namespace.add(dstkey));
    }

    @Override
    public Long rpush(byte[] key, byte[]... strings) {
        return wrapped.rpush(namespace.add(key), strings);
    }

    @Override
    public Long rpush(String key, String... strings) {
        return wrapped.rpush(namespace.add(key), strings);
    }

    @Override
    public Long rpushx(byte[] key, byte[] string) {
        return wrapped.rpushx(namespace.add(key), string);
    }

    @Override
    public Long rpushx(String key, String string) {
        return wrapped.rpushx(namespace.add(key), string);
    }

    @Override
    public Long sadd(byte[] key, byte[]... members) {
        return wrapped.sadd(namespace.add(key), members);
    }

    @Override
    public Long sadd(String key, String... members) {
        return wrapped.sadd(namespace.add(key), members);
    }

    @Override
    public String save() {
        return wrapped.save();
    }

    @Override
    public Long scard(byte[] key) {
        return wrapped.scard(namespace.add(key));
    }

    @Override
    public Long scard(String key) {
        return wrapped.scard(namespace.add(key));
    }

    @Override
    public Set<byte[]> sdiff(byte[]... keys) {
        return wrapped.sdiff(namespace.add(keys));
    }

    @Override
    public Set<String> sdiff(String... keys) {
        return wrapped.sdiff(namespace.add(keys));
    }

    @Override
    public Long sdiffstore(byte[] dstkey, byte[]... keys) {
        return wrapped.sdiffstore(namespace.add(dstkey), namespace.add(keys));
    }

    @Override
    public Long sdiffstore(String dstkey, String... keys) {
        return wrapped.sdiffstore(namespace.add(dstkey), namespace.add(keys));
    }

    @Override
    public String select(int index) {
        return wrapped.select(index);
    }

    @Override
    public String set(byte[] key, byte[] value) {
        return wrapped.set(namespace.add(key), value);
    }

    @Override
    public String set(String key, String value) {
        return wrapped.set(namespace.add(key), value);
    }

    @Override
    public Boolean setbit(byte[] key, long offset, byte[] value) {
        return wrapped.setbit(namespace.add(key), offset, value);
    }

    @Override
    public Boolean setbit(String key, long offset, boolean value) {
        return wrapped.setbit(namespace.add(key), offset, value);
    }

    @Override
    public String setex(byte[] key, int seconds, byte[] value) {
        return wrapped.setex(namespace.add(key), seconds, value);
    }

    @Override
    public String setex(String key, int seconds, String value) {
        return wrapped.setex(namespace.add(key), seconds, value);
    }

    public void setNamespace(String namespace) {
        this.namespace = new NamespaceHandler(namespace);
    }

    @Override
    public Long setnx(byte[] key, byte[] value) {
        return wrapped.setnx(namespace.add(key), value);
    }

    @Override
    public Long setnx(String key, String value) {
        return wrapped.setnx(namespace.add(key), value);
    }

    @Override
    public Long setrange(byte[] key, long offset, byte[] value) {
        return wrapped.setrange(namespace.add(key), offset, value);
    }

    @Override
    public Long setrange(String key, long offset, String value) {
        return wrapped.setrange(namespace.add(key), offset, value);
    }

    @Override
    public String shutdown() {
        return wrapped.shutdown();
    }

    @Override
    public Set<byte[]> sinter(byte[]... keys) {
        return wrapped.sinter(namespace.add(keys));
    }

    @Override
    public Set<String> sinter(String... keys) {
        return wrapped.sinter(namespace.add(keys));
    }

    @Override
    public Long sinterstore(byte[] dstkey, byte[]... keys) {
        return wrapped.sinterstore(namespace.add(dstkey), namespace.add(keys));
    }

    @Override
    public Long sinterstore(String dstkey, String... keys) {
        return wrapped.sinterstore(namespace.add(dstkey), namespace.add(keys));
    }

    @Override
    public Boolean sismember(byte[] key, byte[] member) {
        return wrapped.sismember(namespace.add(key), member);
    }

    @Override
    public Boolean sismember(String key, String member) {
        return wrapped.sismember(namespace.add(key), member);
    }

    @Override
    public String slaveof(String host, int port) {
        return wrapped.slaveof(host, port);
    }

    @Override
    public String slaveofNoOne() {
        return wrapped.slaveofNoOne();
    }

    @Override
    public Set<byte[]> smembers(byte[] key) {
        return wrapped.smembers(namespace.add(key));
    }

    @Override
    public Set<String> smembers(String key) {
        return wrapped.smembers(namespace.add(key));
    }

    @Override
    public Long smove(byte[] srckey, byte[] dstkey, byte[] member) {
        return wrapped.smove(namespace.add(srckey), namespace.add(dstkey), member);
    }

    @Override
    public Long smove(String srckey, String dstkey, String member) {
        return wrapped.smove(namespace.add(srckey), namespace.add(dstkey), member);
    }

    @Override
    public List<byte[]> sort(byte[] key) {
        return wrapped.sort(namespace.add(key));
    }

    @Override
    public Long sort(byte[] key, byte[] dstkey) {
        return wrapped.sort(namespace.add(key), namespace.add(dstkey));
    }

    @Override
    public List<byte[]> sort(byte[] key, SortingParams sortingParameters) {
        return wrapped.sort(namespace.add(key), namespace.add(sortingParameters));
    }

    @Override
    public Long sort(byte[] key, SortingParams sortingParameters, byte[] dstkey) {
        return wrapped.sort(namespace.add(key), namespace.add(sortingParameters), namespace.add(dstkey));
    }

    @Override
    public List<String> sort(String key) {
        return wrapped.sort(namespace.add(key));
    }

    @Override
    public List<String> sort(String key, SortingParams sortingParameters) {
        return wrapped.sort(namespace.add(key), namespace.add(sortingParameters));
    }

    @Override
    public Long sort(String key, SortingParams sortingParameters, String dstkey) {
        return wrapped.sort(namespace.add(key), namespace.add(sortingParameters), namespace.add(dstkey));
    }

    @Override
    public Long sort(String key, String dstkey) {
        return wrapped.sort(namespace.add(key), namespace.add(dstkey));
    }

    @Override
    public byte[] spop(byte[] key) {
        return wrapped.spop(namespace.add(key));
    }

    @Override
    public String spop(String key) {
        return wrapped.spop(namespace.add(key));
    }

    @Override
    public byte[] srandmember(byte[] key) {
        return wrapped.srandmember(namespace.add(key));
    }

    @Override
    public String srandmember(String key) {
        return wrapped.srandmember(namespace.add(key));
    }

    @Override
    public Long srem(byte[] key, byte[]... members) {
        return wrapped.srem(namespace.add(key), members);
    }

    @Override
    public Long srem(String key, String... members) {
        return wrapped.srem(namespace.add(key), members);
    }

    @Override
    public Long strlen(byte[] key) {
        return wrapped.strlen(namespace.add(key));
    }

    @Override
    public Long strlen(String key) {
        return wrapped.strlen(namespace.add(key));
    }

    @Override
    public void subscribe(BinaryJedisPubSub jedisPubSub, byte[]... channels) {
        wrapped.subscribe(jedisPubSub, channels);
    }

    @Override
    public void subscribe(JedisPubSub jedisPubSub, String... channels) {
        wrapped.subscribe(jedisPubSub, channels);
    }

    @Override
    public byte[] substr(byte[] key, int start, int end) {
        return wrapped.substr(namespace.add(key), start, end);
    }

    @Override
    public String substr(String key, int start, int end) {
        return wrapped.substr(namespace.add(key), start, end);
    }

    @Override
    public Set<byte[]> sunion(byte[]... keys) {
        return wrapped.sunion(namespace.add(keys));
    }

    @Override
    public Set<String> sunion(String... keys) {
        return wrapped.sunion(namespace.add(keys));
    }

    @Override
    public Long sunionstore(byte[] dstkey, byte[]... keys) {
        return wrapped.sunionstore(namespace.add(dstkey), namespace.add(keys));
    }

    @Override
    public Long sunionstore(String dstkey, String... keys) {
        return wrapped.sunionstore(namespace.add(dstkey), namespace.add(keys));
    }

    @Override
    public void sync() {
        wrapped.sync();
    }

    @Override
    public Long ttl(byte[] key) {
        return wrapped.ttl(namespace.add(key));
    }

    @Override
    public Long ttl(String key) {
        return wrapped.ttl(namespace.add(key));
    }

    @Override
    public String type(byte[] key) {
        return wrapped.type(namespace.add(key));
    }

    @Override
    public String type(String key) {
        return wrapped.type(namespace.add(key));
    }

    @Override
    public String unwatch() {
        return wrapped.unwatch();
    }

    @Override
    public String watch(byte[]... keys) {
        return wrapped.watch(namespace.add(keys));
    }

    @Override
    public String watch(String... keys) {
        return wrapped.watch(namespace.add(keys));
    }

    public NamespaceJedis withNamespace(String ns) {
        setNamespace(ns);
        return this;
    }

    @Override
    public Long zadd(byte[] key, double score, byte[] member) {
        return wrapped.zadd(namespace.add(key), score, member);
    }

    @Override
    public Long zadd(String key, double score, String member) {
        return wrapped.zadd(namespace.add(key), score, member);
    }

    @Override
    public Long zcard(byte[] key) {
        return wrapped.zcard(namespace.add(key));
    }

    @Override
    public Long zcard(String key) {
        return wrapped.zcard(namespace.add(key));
    }

    @Override
    public Long zcount(byte[] key, double min, double max) {
        return wrapped.zcount(namespace.add(key), min, max);
    }

    @Override
    public Long zcount(String key, double min, double max) {
        return wrapped.zcount(namespace.add(key), min, max);
    }

    @Override
    public Double zincrby(byte[] key, double score, byte[] member) {
        return wrapped.zincrby(namespace.add(key), score, member);
    }

    @Override
    public Double zincrby(String key, double score, String member) {
        return wrapped.zincrby(namespace.add(key), score, member);
    }

    @Override
    public Long zinterstore(byte[] dstkey, byte[]... sets) {
        return wrapped.zinterstore(namespace.add(dstkey), namespace.add(sets));
    }

    @Override
    public Long zinterstore(byte[] dstkey, ZParams params, byte[]... sets) {
        return wrapped.zinterstore(namespace.add(dstkey), params, namespace.add(sets));
    }

    @Override
    public Long zinterstore(String dstkey, String... sets) {
        return wrapped.zinterstore(namespace.add(dstkey), namespace.add(sets));
    }

    @Override
    public Long zinterstore(String dstkey, ZParams params, String... sets) {
        return wrapped.zinterstore(namespace.add(dstkey), params, namespace.add(sets));
    }

    @Override
    public Set<byte[]> zrange(byte[] key, int start, int end) {
        return wrapped.zrange(namespace.add(key), start, end);
    }

    @Override
    public Set<String> zrange(String key, long start, long end) {
        return wrapped.zrange(namespace.add(key), start, end);
    }

    @Override
    public Set<byte[]> zrangeByScore(byte[] key, byte[] min, byte[] max) {
        return wrapped.zrangeByScore(namespace.add(key), min, max);
    }

    @Override
    public Set<byte[]> zrangeByScore(byte[] key, double min, double max) {
        return wrapped.zrangeByScore(namespace.add(key), min, max);
    }

    @Override
    public Set<byte[]> zrangeByScore(byte[] key, double min, double max, int offset, int count) {
        return wrapped.zrangeByScore(namespace.add(key), min, max, offset, count);
    }

    @Override
    public Set<String> zrangeByScore(String key, double min, double max) {
        return wrapped.zrangeByScore(namespace.add(key), min, max);
    }

    @Override
    public Set<String> zrangeByScore(String key, double min, double max, int offset, int count) {
        return wrapped.zrangeByScore(namespace.add(key), min, max, offset, count);
    }

    @Override
    public Set<String> zrangeByScore(String key, String min, String max) {
        return wrapped.zrangeByScore(namespace.add(key), min, max);
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(byte[] key, double min, double max) {
        return wrapped.zrangeByScoreWithScores(namespace.add(key), min, max);
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(byte[] key, double min, double max, int offset, int count) {
        return wrapped.zrangeByScoreWithScores(namespace.add(key), min, max, offset, count);
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(String key, double min, double max) {
        return wrapped.zrangeByScoreWithScores(namespace.add(key), min, max);
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(String key, double min, double max, int offset, int count) {
        return wrapped.zrangeByScoreWithScores(namespace.add(key), min, max, offset, count);
    }

    @Override
    public Set<Tuple> zrangeWithScores(byte[] key, int start, int end) {
        return wrapped.zrangeWithScores(namespace.add(key), start, end);
    }

    @Override
    public Set<Tuple> zrangeWithScores(String key, long start, long end) {
        return wrapped.zrangeWithScores(namespace.add(key), start, end);
    }

    @Override
    public Long zrank(byte[] key, byte[] member) {
        return wrapped.zrank(namespace.add(key), member);
    }

    @Override
    public Long zrank(String key, String member) {
        return wrapped.zrank(namespace.add(key), member);
    }

    @Override
    public Long zrem(byte[] key, byte[]... members) {
        return wrapped.zrem(namespace.add(key), members);
    }

    @Override
    public Long zrem(String key, String... members) {
        return wrapped.zrem(namespace.add(key), members);
    }

    @Override
    public Long zremrangeByRank(byte[] key, int start, int end) {
        return wrapped.zremrangeByRank(namespace.add(key), start, end);
    }

    @Override
    public Long zremrangeByRank(String key, long start, long end) {
        return wrapped.zremrangeByRank(namespace.add(key), start, end);
    }

    @Override
    public Long zremrangeByScore(byte[] key, double start, double end) {
        return wrapped.zremrangeByScore(namespace.add(key), start, end);
    }

    @Override
    public Long zremrangeByScore(String key, double start, double end) {
        return wrapped.zremrangeByScore(namespace.add(key), start, end);
    }

    @Override
    public Set<byte[]> zrevrange(byte[] key, int start, int end) {
        return wrapped.zrevrange(namespace.add(key), start, end);
    }

    @Override
    public Set<String> zrevrange(String key, long start, long end) {
        return wrapped.zrevrange(namespace.add(key), start, end);
    }

    @Override
    public Set<byte[]> zrevrangeByScore(byte[] key, byte[] max, byte[] min) {
        return wrapped.zrevrangeByScore(namespace.add(key), max, min);
    }

    @Override
    public Set<byte[]> zrevrangeByScore(byte[] key, double max, double min) {
        return wrapped.zrevrangeByScore(namespace.add(key), max, min);
    }

    @Override
    public Set<byte[]> zrevrangeByScore(byte[] key, double max, double min, int offset, int count) {
        return wrapped.zrevrangeByScore(namespace.add(key), max, min, offset, count);
    }

    @Override
    public Set<String> zrevrangeByScore(String key, double max, double min) {
        return wrapped.zrevrangeByScore(namespace.add(key), max, min);
    }

    @Override
    public Set<String> zrevrangeByScore(String key, double max, double min, int offset, int count) {
        return wrapped.zrevrangeByScore(namespace.add(key), max, min, offset, count);
    }

    @Override
    public Set<String> zrevrangeByScore(String key, String max, String min) {
        return wrapped.zrevrangeByScore(namespace.add(key), max, min);
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(byte[] key, double max, double min) {
        return wrapped.zrevrangeByScoreWithScores(namespace.add(key), max, min);
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(byte[] key, double max, double min, int offset, int count) {
        return wrapped.zrevrangeByScoreWithScores(namespace.add(key), max, min, offset, count);
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(String key, double max, double min) {
        return wrapped.zrevrangeByScoreWithScores(namespace.add(key), max, min);
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(String key, double max, double min, int offset, int count) {
        return wrapped.zrevrangeByScoreWithScores(namespace.add(key), max, min, offset, count);
    }

    @Override
    public Set<Tuple> zrevrangeWithScores(byte[] key, int start, int end) {
        return wrapped.zrevrangeWithScores(namespace.add(key), start, end);
    }

    @Override
    public Set<Tuple> zrevrangeWithScores(String key, long start, long end) {
        return wrapped.zrevrangeWithScores(namespace.add(key), start, end);
    }

    @Override
    public Long zrevrank(byte[] key, byte[] member) {
        return wrapped.zrevrank(namespace.add(key), member);
    }

    @Override
    public Long zrevrank(String key, String member) {
        return wrapped.zrevrank(namespace.add(key), member);
    }

    @Override
    public Double zscore(byte[] key, byte[] member) {
        return wrapped.zscore(namespace.add(key), member);
    }

    @Override
    public Double zscore(String key, String member) {
        return wrapped.zscore(namespace.add(key), member);
    }

    @Override
    public Long zunionstore(byte[] dstkey, byte[]... sets) {
        return wrapped.zunionstore(namespace.add(dstkey), namespace.add(sets));
    }

    @Override
    public Long zunionstore(byte[] dstkey, ZParams params, byte[]... sets) {
        return wrapped.zunionstore(namespace.add(dstkey), params, namespace.add(sets));
    }

    @Override
    public Long zunionstore(String dstkey, String... sets) {
        return wrapped.zunionstore(namespace.add(dstkey), namespace.add(sets));
    }

    @Override
    public Long zunionstore(String dstkey, ZParams params, String... sets) {
        return wrapped.zunionstore(namespace.add(dstkey), params, namespace.add(sets));
    }
}
