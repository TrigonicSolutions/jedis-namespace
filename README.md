# Jedis Namespace

This adds namespaces to Jedis, similar to
[Redis::Namespace](https://github.com/defunkt/redis-namespace)

Currently based on Jedis 2.0.0

## Usage

To use, you construct a `NamespaceJedis` instance with the same constructor
parameters as `Jedis`.  You can then specify a namespace to use for all keys.

    import com.trigonic.jedis.NamespaceJedis;

    // ...

    Jedis jedis = new NamespaceJedis("localhost").withNamespace("ns");
    jedis.set("foo", "bar")

This will result in the following Redis command being executed:

    SET ns:foo bar

Specified keys have the namespace prefixed to them before being given to
the Redis server, and returned keys will have the namespace removed.

Similarly, you can construct a `NamespaceJedisPool` where you would have
constructed a `JedisPool`.

