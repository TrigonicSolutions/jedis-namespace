package com.trigonic.jedis;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static redis.clients.jedis.BinaryClient.LIST_POSITION.AFTER;
import static redis.clients.jedis.BinaryClient.LIST_POSITION.BEFORE;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.SortingParams;
import redis.clients.jedis.ZParams;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;

public class NamespaceJedisTest {
    private Jedis jedis;
    private NamespaceJedisPool namespacedPool;
    private Jedis namespaced;

    @Before
    public void setup() {
        jedis = new Jedis("localhost");
        jedis.flushDB();

        namespacedPool = new NamespaceJedisPool("localhost").withNamespace("ns");
        namespaced = namespacedPool.getResource();

        jedis.set("foo", "bar");
    }

    @After
    public void tearDown() {
        namespacedPool.returnResource(namespaced);

        jedis.flushDB();
        jedis.quit();
    }

    @Test
    public void shouldBeAbleToUseANamespace() {
        assertNull(namespaced.get("foo"));

        namespaced.set("foo", "chris");
        assertEquals("bar", jedis.get("foo"));
        assertEquals("chris", namespaced.get("foo"));
        assertEquals("chris", jedis.get("ns:foo"));

        jedis.set("foo", "bob");
        assertEquals("bob", jedis.get("foo"));
        assertEquals("chris", namespaced.get("foo"));
        assertEquals("chris", jedis.get("ns:foo"));

        namespaced.incrBy("counter", 2);
        assertEquals("2", namespaced.get("counter"));
        assertNull(jedis.get("counter"));
        assertEquals("string", namespaced.type("counter"));
    }

    @Test
    public void shouldBeAbleToUseANamespaceWithBlpop() {
        namespaced.rpush("foo", "string");
        namespaced.rpush("foo", "ns:string");
        assertEquals(Arrays.asList("foo", "string"), namespaced.blpop(1, "foo"));
        assertEquals(Arrays.asList("foo", "ns:string"), namespaced.blpop(1, "foo"));
        assertNull(namespaced.blpop(1, "foo"));
    }

    @Test
    public void shouldBeAbleToUseANamespaceWithBrpop() {
        namespaced.rpush("foo", "string");
        namespaced.rpush("foo", "ns:string");
        assertEquals(Arrays.asList("foo", "ns:string"), namespaced.brpop(1, "foo"));
        assertEquals(Arrays.asList("foo", "string"), namespaced.brpop(1, "foo"));
        assertNull(namespaced.blpop(1, "foo"));
    }

    @Test
    public void shouldBeAbleToUseANamespaceWithBrpoplpush() {
        namespaced.rpush("foo", "string");
        namespaced.rpush("foo", "ns:string");
        namespaced.brpoplpush("foo", "bar", 1);
        assertEquals(Arrays.asList("foo", "string"), namespaced.brpop(1, "foo"));
        assertNull(namespaced.blpop(1, "foo"));
        assertEquals(Arrays.asList("bar", "ns:string"), namespaced.brpop(1, "bar"));
        assertNull(namespaced.blpop(1, "bar"));
    }

    @Test
    public void shouldBeAbleToUseANamespaceWithDel() {
        namespaced.set("foo", "1000");
        namespaced.set("bar", "2000");
        namespaced.set("baz", "3000");
        assertEquals(asSet("foo", "bar", "baz"), namespaced.keys("*"));
        namespaced.del("foo");
        assertNull(namespaced.get("foo"));
        namespaced.del("bar", "baz");
        assertNull(namespaced.get("bar"));
        assertNull(namespaced.get("baz"));
        assertEquals(asSet(), namespaced.keys("*"));
    }

    @Test
    public void shouldBeAbleToUseANamespaceWithDelPipelined() {
        Pipeline pipeline = namespaced.pipelined();
        pipeline.set("foo", "1000");
        pipeline.set("bar", "2000");
        pipeline.set("baz", "3000");
        Response<Set<String>> response1 = pipeline.keys("*");
        
        pipeline.del("foo");
        Response<String> response2 = pipeline.get("foo");
        
        pipeline.del("bar", "baz");
        Response<String> response3 = pipeline.get("bar");
        Response<String> response4 = pipeline.get("baz");
        Response<Set<String>> response5 = pipeline.keys("*");
        
        pipeline.sync();

        assertEquals(asSet("foo", "bar", "baz"), response1.get());
        assertNull(response2.get());
        assertNull(response3.get());
        assertNull(response4.get());
        assertEquals(asSet(), response5.get());
    }

    @Test
    public void shouldBeAbleToUseANamespaceWithMget() {
        namespaced.set("foo", "1000");
        namespaced.set("bar", "2000");
        assertEquals(asList("1000", "2000"), namespaced.mget("foo", "bar"));
        assertEquals(asList("1000", "2000", null), namespaced.mget("foo", "bar", "baz"));
    }

    @Test
    public void shouldBeAbleToUseANamespaceWithMset() {
        namespaced.mset("foo", "1000", "bar", "2000");
        assertEquals(asList("1000", "2000"), namespaced.mget("foo", "bar"));
        assertEquals(asList("1000", "2000", null), namespaced.mget("foo", "bar", "baz"));
    }

    @Test
    public void shouldBeAbleToUseANamespaceWithMsetnx() {
        namespaced.msetnx("foo", "1000", "bar", "2000");
        assertEquals(asList("1000", "2000"), namespaced.mget("foo", "bar"));
        assertEquals(asList("1000", "2000", null), namespaced.mget("foo", "bar", "baz"));
    }

    @Test
    public void shouldBeAbleToUseANamespaceWithHashes() {
        namespaced.hset("foo", "key", "value");
        namespaced.hset("foo", "key1", "value1");
        assertEquals("value", namespaced.hget("foo", "key"));
        assertEquals(asMap("key", "value", "key1", "value1"), namespaced.hgetAll("foo"));
        assertEquals(Long.valueOf(2), namespaced.hlen("foo"));
        assertEquals(asSet("key", "key1"), namespaced.hkeys("foo"));

        namespaced.hmset("bar", asMap("key", "value", "key1", "value1"));
        namespaced.hmget("bar", "key", "key1");
        namespaced.hmset("bar", asMap("a_number", "1"));
        assertEquals(asList("1"), namespaced.hmget("bar", "a_number"));

        namespaced.hincrBy("bar", "a_number", 3);
        assertEquals(asList("4"), namespaced.hmget("bar", "a_number"));
        assertEquals(asMap("key", "value", "key1", "value1", "a_number", "4"), namespaced.hgetAll("bar"));

        assertEquals(Long.valueOf(1), namespaced.hsetnx("foonx", "nx", "10"));
        assertEquals(Long.valueOf(0), namespaced.hsetnx("foonx", "nx", "12"));
        assertEquals("10", namespaced.hget("foonx", "nx"));
        assertEquals(asSet("nx"), namespaced.hkeys("foonx"));
        assertEquals(asList("10"), namespaced.hvals("foonx"));
        namespaced.hmset("baz", asMap("key", "value", "key1", "value1", "a_number", "4"));
        assertEquals(asMap("key", "value", "key1", "value1", "a_number", "4"), namespaced.hgetAll("baz"));
    }
    
    @Test
    public void shouldProperlyMoveMemberBetweenSets() {
        namespaced.sadd("foo", "1");
        namespaced.sadd("foo", "2");
        namespaced.sadd("foo", "3");
        namespaced.sadd("bar", "2");
        namespaced.sadd("bar", "3");
        assertEquals(asSet("1", "2", "3"), namespaced.smembers("foo"));
        assertEquals(asSet("2", "3"), namespaced.smembers("bar"));
        
        namespaced.smove("foo", "bar", "1");
        assertEquals(asSet("2", "3"), namespaced.smembers("foo"));
        assertEquals(asSet("1", "2", "3"), namespaced.smembers("bar"));
    }

    @Test
    public void shouldProperlyIntersectThreeSets() {
        namespaced.sadd("foo", "1");
        namespaced.sadd("foo", "2");
        namespaced.sadd("foo", "3");
        namespaced.sadd("bar", "2");
        namespaced.sadd("bar", "3");
        namespaced.sadd("bar", "4");
        namespaced.sadd("baz", "3");
        assertEquals(asSet("3"), namespaced.sinter("foo", "bar", "baz"));
    }

    @Test
    public void shouldProperlyUnionTwoSets() {
        namespaced.sadd("foo", "1");
        namespaced.sadd("foo", "2");
        namespaced.sadd("bar", "2");
        namespaced.sadd("bar", "3");
        namespaced.sadd("bar", "4");
        assertEquals(asSet("1", "2", "3", "4"), namespaced.sunion("foo", "bar"));
    }

    @Test
    public void shouldProperlyUnionTwoSortedSetsWithOptions() {
        namespaced.zadd("sort1", 1, "1");
        namespaced.zadd("sort1", 2, "2");
        namespaced.zadd("sort2", 2, "2");
        namespaced.zadd("sort2", 3, "3");
        namespaced.zadd("sort2", 4, "4");
        namespaced.zunionstore("union", new ZParams().weights(2, 1), "sort1", "sort2");
        assertEquals(asSet("2", "4", "3", "1"), namespaced.zrevrange("union", 0, -1));
    }

    @Test
    public void shouldProperlyUnionTwoSortedSetsWithoutOptions() {
        namespaced.zadd("sort1", 1, "1");
        namespaced.zadd("sort1", 2, "2");
        namespaced.zadd("sort2", 2, "2");
        namespaced.zadd("sort2", 3, "3");
        namespaced.zadd("sort2", 4, "4");
        namespaced.zunionstore("union", "sort1", "sort2");
        assertEquals(asSet("4", "2", "3", "1"), namespaced.zrevrange("union", 0, -1));
    }

    @Test
    public void shouldAddNamespaceToSort() {
        namespaced.sadd("foo", "1");
        namespaced.sadd("foo", "2");
        namespaced.set("weight_1", "2");
        namespaced.set("weight_2", "1");
        namespaced.set("value_1", "a");
        namespaced.set("value_2", "b");

        assertEquals(asList("1", "2"), namespaced.sort("foo"));
        assertEquals(asList("1"), namespaced.sort("foo", new SortingParams().limit(0, 1)));
        assertEquals(asList("2", "1"), namespaced.sort("foo", new SortingParams().desc()));
        assertEquals(asList("2", "1"), namespaced.sort("foo", new SortingParams().by("weight_*")));
        assertEquals(asList("a", "b"), namespaced.sort("foo", new SortingParams().get("value_*")));

        namespaced.sort("foo", "result");
        assertEquals(asList("1", "2"), namespaced.lrange("result", 0, -1));
    }

    @Test
    public void shouldYieldTheCorrectListOfKeys() {
        namespaced.set("foo", "1");
        namespaced.set("bar", "2");
        namespaced.set("baz", "3");
        assertEquals(asSet("bar", "baz", "foo"), namespaced.keys("*"));
    }

    @Test
    public void canChangeItsNamespace() {
        assertNull(namespaced.get("foo"));
        namespaced.set("foo", "chris");
        assertEquals("chris", namespaced.get("foo"));

        assertEquals("ns", ((NamespaceJedis)namespaced).getNamespace());
        ((NamespaceJedis)namespaced).setNamespace("test");
        assertEquals("test", ((NamespaceJedis)namespaced).getNamespace());

        assertNull(namespaced.get("foo"));
        namespaced.set("foo", "chris");
        assertEquals("chris", namespaced.get("foo"));
    }
    
    @Test
    public void poolCanChangeItsNamespace() {
        assertNull(namespaced.get("foo"));
        namespaced.set("foo", "chris");
        assertEquals("chris", namespaced.get("foo"));

        assertEquals("ns", ((NamespaceJedis)namespaced).getNamespace());
        ((NamespaceJedis)namespaced).setNamespace("test");
        assertEquals("test", ((NamespaceJedis)namespaced).getNamespace());

        assertNull(namespaced.get("foo"));
        namespaced.set("foo", "alice");
        assertEquals("alice", namespaced.get("foo"));
        
        Jedis secondNamespaced = namespacedPool.getResource();
        assertEquals("chris", secondNamespaced.get("foo"));        
        assertEquals("alice", namespaced.get("foo"));       
        
        namespacedPool.returnResource(secondNamespaced);
    }
    
    @Test
    public void shouldBeAbleToUseANamespaceWithAppend() {
        assertNull(namespaced.get("foo"));
        
        namespaced.append("foo", "foo");
        namespaced.append("foo", "bar");
        assertEquals("foobar", namespaced.get("foo"));
        assertEquals("foobar", jedis.get("ns:foo"));
    }
    
    @Test
    public void shouldBeAbleToUseANamespaceWithIncrDecr() {
        assertNull(namespaced.get("foo"));

        namespaced.incr("foo");
        assertEquals("1", namespaced.get("foo"));
        
        namespaced.incrBy("foo", 3);
        assertEquals("4", namespaced.get("foo"));
        
        namespaced.decr("foo");
        assertEquals("3", namespaced.get("foo"));
        
        namespaced.decrBy("foo", 3);
        assertEquals("0", namespaced.get("foo"));
    }
    
    @Test
    public void shouldBeAbleToUseANamespaceWithLists() {
        assertNull(namespaced.get("foo"));
        
        namespaced.lpushx("foo", "bar");
        assertNull(namespaced.get("foo"));
        
        namespaced.lpush("foo", "20");
        namespaced.lpush("foo", "10");
        namespaced.linsert("foo", AFTER, "20", "40");
        namespaced.linsert("foo", BEFORE, "40", "30");
        assertEquals(Long.valueOf(4), namespaced.llen("foo"));
        assertEquals("10", namespaced.lindex("foo", 0));
        assertEquals(asList("20", "30"), namespaced.lrange("foo", 1, 2));
        
        namespaced.lset("foo", 0, "40");
        namespaced.lrem("foo", 1, "40");
        namespaced.ltrim("foo", 1, 2);
        assertEquals(asList("30", "40"), namespaced.lrange("foo", 0, 3));
    }

    @Test
    public void shouldBeAbleToUseANamespaceWithListsPipelined() {
        Pipeline pipeline = namespaced.pipelined();
        Response<String> response1 = pipeline.get("foo");
        
        pipeline.lpushx("foo", "bar");
        Response<String> response2 = pipeline.get("foo");
        
        pipeline.lpush("foo", "20");
        pipeline.lpush("foo", "10");
        pipeline.linsert("foo", AFTER, "20", "40");
        pipeline.linsert("foo", BEFORE, "40", "30");
        Response<Long> response3 = pipeline.llen("foo");
        Response<String> response4 = pipeline.lindex("foo", 0);
        Response<List<String>> response5 = pipeline.lrange("foo", 1, 2);
        
        pipeline.lset("foo", 0, "40");
        pipeline.lrem("foo", 1, "40");
        pipeline.ltrim("foo", 1, 2);
        Response<List<String>> response6 = pipeline.lrange("foo", 0, 3);
        
        pipeline.sync();
        
        assertNull(response1.get());
        assertNull(response2.get());        
        assertEquals(Long.valueOf(4), response3.get());
        assertEquals("10", response4.get());
        assertEquals(asList("20", "30"), response5.get());
        assertEquals(asList("30", "40"), response6.get());
    }
    
    @Test
    public void canReturnAndCheckoutUsableResource() {
        namespaced.set("breadcrumb", "something");
        assertEquals("something", jedis.get("ns:breadcrumb"));
        namespacedPool.returnResource(namespaced);
        
        namespaced = namespacedPool.getResource();
        assertEquals("something", namespaced.get("breadcrumb"));
    }

    @Test
    public void canCheckoutMultipleUsableResource() {
        namespaced.set("breadcrumb", "somethingElse");
        assertEquals("somethingElse", jedis.get("ns:breadcrumb"));
        
        Jedis other = namespacedPool.getResource();
        assertEquals("somethingElse", other.get("breadcrumb"));
        namespacedPool.returnResource(other);
    }

    protected static <T> Set<T> asSet(T... values) {
        return new HashSet<T>(Arrays.asList(values));
    }

    protected static <T> Map<T, T> asMap(T... keyvalues) {
        Builder<T, T> builder = ImmutableMap.builder();
        for (int i = 0; i < keyvalues.length; i += 2) {
            T key = keyvalues[i], value = null;
            if (i + 1 < keyvalues.length) {
                value = keyvalues[i + 1];
            }
            builder.put(key, value);
        }
        return builder.build();
    }
}
