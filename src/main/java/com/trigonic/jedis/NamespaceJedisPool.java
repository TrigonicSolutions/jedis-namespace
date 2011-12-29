package com.trigonic.jedis;

import static redis.clients.jedis.Protocol.DEFAULT_PORT;
import static redis.clients.jedis.Protocol.DEFAULT_TIMEOUT;

import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.pool.impl.GenericObjectPool.Config;

import redis.clients.jedis.Jedis;
import redis.clients.util.Pool;

public class NamespaceJedisPool extends Pool<Jedis> {
    private AtomicReference<String> namespace;

    public NamespaceJedisPool(final Config poolConfig, final String host, int port, int timeout, final String password) {
        this(poolConfig, host, port, timeout, password, new AtomicReference<String>());
    }

    public NamespaceJedisPool(final Config poolConfig, final String host) {
        this(poolConfig, host, DEFAULT_PORT);
    }

    public NamespaceJedisPool(final Config poolConfig, final String host, final int port) {
        this(poolConfig, host, port, DEFAULT_TIMEOUT);
    }

    public NamespaceJedisPool(final Config poolConfig, final String host, final int port, final int timeout) {
        this(poolConfig, host, port, timeout, null);
    }

    public NamespaceJedisPool(final String host) {
        this(host, DEFAULT_PORT);
    }

    public NamespaceJedisPool(final String host, final int port) {
        this(host, port, DEFAULT_TIMEOUT);
    }
    
    public NamespaceJedisPool(final String host, final int port, final int timeout) {
        this(new Config(), host, port, timeout, null);
    }
    
    protected NamespaceJedisPool(final Config poolConfig, final String host, int port, int timeout, final String password, final AtomicReference<String> namespace) {
        super(poolConfig, new NamespaceJedisFactory(host, port, timeout, password, namespace));
        this.namespace = namespace;
    }

    public void setNamespace(String namespace) {        
        this.namespace.set(namespace);
    }
    
    public NamespaceJedisPool withNamespace(String ns) {
        setNamespace(ns);
        return this;
    }
}
