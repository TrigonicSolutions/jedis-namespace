package com.trigonic.jedis;

import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.pool.BasePoolableObjectFactory;

import redis.clients.jedis.Jedis;

public class NamespaceJedisFactory extends BasePoolableObjectFactory {
    private final String host;
    private final int port;
    private final int timeout;
    private final String password;
    private final AtomicReference<String> namespace;

    public NamespaceJedisFactory(final String host, final int port, final int timeout, final String password, final AtomicReference<String> namespace) {
        this.host = host;
        this.port = port;
        this.timeout = (timeout > 0) ? timeout : -1;
        this.password = password;
        this.namespace = namespace;
    }

    @Override
    public Object makeObject() throws Exception {
        final NamespaceJedis jedis;
        if (timeout > 0) {
            jedis = new NamespaceJedis(namespace.get(), new Jedis(this.host, this.port, this.timeout));
        } else {
            jedis = new NamespaceJedis(namespace.get(), new Jedis(this.host, this.port));
        }

        jedis.connect();
        if (null != this.password) {
            jedis.auth(this.password);
        }
        return jedis;
    }
    
    @Override
    public void activateObject(Object obj) throws Exception {
        super.activateObject(obj);
        resetNamespace(obj);
    }
    
    @Override
    public void passivateObject(Object obj) throws Exception {
        super.passivateObject(obj);
        resetNamespace(obj);
    }

    /**
     * Ensure a {@link NamespaceJedis} handed out from the pool has the pool's current namespace on it.
     */
    protected void resetNamespace(Object obj) {
        if (obj instanceof NamespaceJedis) {
            final NamespaceJedis jedis = (NamespaceJedis) obj;
            jedis.setNamespace(namespace.get());
        }
    }

    @Override
    public void destroyObject(final Object obj) throws Exception {
        if (obj instanceof Jedis) {
            final Jedis jedis = (Jedis) obj;
            if (jedis.isConnected()) {
                try {
                    try {
                        jedis.quit();
                    } catch (Exception e) {
                        // ignore
                    }
                    jedis.disconnect();
                } catch (Exception e) {
                    // ignore
                }
            }
        }
    }

    @Override
    public boolean validateObject(final Object obj) {
        boolean result = false;
        if (obj instanceof Jedis) {
            final Jedis jedis = (Jedis) obj;
            try {
                result = jedis.isConnected() && jedis.ping().equals("PONG");
            } catch (final Exception e) {
                result = false;
            }
        }
        return result;
    }

}