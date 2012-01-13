package com.trigonic.jedis;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import redis.clients.jedis.Response;
import redis.clients.jedis.SortingParams;
import redis.clients.util.SafeEncoder;

import com.google.common.primitives.Bytes;

public class NamespaceHandler {
    private String namespace;
    private String prefix;
    private byte[] binaryPrefix;

    public NamespaceHandler(String namespace) {
        this.namespace = namespace;
        prefix = namespace + ":";
        binaryPrefix = SafeEncoder.encode(prefix);        
    }
    
    public String get() {
        return namespace;
    }
    
    public String add(String key) {
        return key == null ? null : prefix + key;
    }
    
    public String[] add(String[] keys) {
        String[] result = null;
        if (keys != null) {
            result = new String[keys.length];
            for (int i = 0; i < keys.length; ++i) {
                result[i] = add(keys[i]);
            }
        }
        return result;
    }
    
    public String[] addEveryOther(String[] keys) {
        String[] result = null;
        if (keys != null) {
            result = new String[keys.length];
            for (int i = 0; i < keys.length; ++i) {
                result[i] = (i % 2 == 0 ? add(keys[i]) : keys[i]);
            }
        }
        return result;
    }
    
    public byte[][] addEveryOther(byte[][] keys) {
        byte[][] result = null;
        if (keys != null) {
            result = new byte[keys.length][];
            for (int i = 0; i < keys.length; ++i) {
                result[i] = (i % 2 == 0 ? add(keys[i]) : keys[i]);
            }
        }
        return result;
    }
    
    public byte[] add(byte[] key) {
        return key == null ? null : Bytes.concat(binaryPrefix, key);
    }
    
    public byte[][] add(byte[][] keys) {
        byte[][] result = null;
        if (keys != null) {
            result = new byte[keys.length][];
            for (int i = 0; i < keys.length; ++i) {
                result[i] = add(keys[i]);
            }
        }
        return result;
    }
    
    public SortingParams add(SortingParams params) {
        List<byte[]> rawParams = new ArrayList<byte[]>(params.getParams());
        SortingParams result = new SortingParams();
        for (int i = 0; i < rawParams.size(); ++i) {
            String paramName = SafeEncoder.encode(rawParams.get(i));
            if (paramName.equals("alpha")) {
                result.alpha();
            } else if (paramName.equals("asc")) {
                result.asc();
            } else if (paramName.equals("by")) {
                result.by(add(rawParams.get(++i)));
            } else if (paramName.equals("desc")) {
                result.desc();
            } else if (paramName.equals("get")) {
                result.get(add(rawParams.get(++i)));
            } else if (paramName.equals("limit")) {
                int start = Integer.valueOf(SafeEncoder.encode(rawParams.get(++i)));
                int count = Integer.valueOf(SafeEncoder.encode(rawParams.get(++i)));
                result.limit(start, count);
            } else if (paramName.equals("nosort")) {
                result.nosort();
            }
        }
        return result;
    }
    
    public String remove(String key) {
        if (key != null && key.startsWith(prefix)) {
            key = key.substring(prefix.length());
        }
        return key;
    }
    
    public Set<String> remove(Set<String> keys) {
        Set<String> result = null;
        if (keys != null) {
            result = new LinkedHashSet<String>(keys.size());
            for (String key : keys) {
                result.add(remove(key));
            }
        }
        return result;
    }
    
    public Response<Set<String>> remove(Response<Set<String>> response) {
        return new FilteredResponse<Set<String>>(response) {
            @Override
            protected Set<String> filter(Set<String> value) {
                return remove(value);
            }
        };
    }
    
    public byte[] removeNamespace(byte[] key) {
        if (key != null && Bytes.indexOf(key, binaryPrefix) == 0) {
            key = Arrays.copyOfRange(key, binaryPrefix.length, key.length);
        }
        return key;
    }
    
    public List<String> removeFirst(List<String> list) {
        List<String> result = null;
        if (list != null) {
            result = new ArrayList<String>(list.size());
            result.addAll(list);
            result.set(0, remove(result.get(0)));
        }
        return result;
    }
    
    public Response<List<String>> removeFirst(Response<List<String>> response) {
        return new FilteredResponse<List<String>>(response) {
            @Override
            protected List<String> filter(List<String> list) {
                return removeFirst(list);
            }
        };
    }

    public List<byte[]> removeFirstBytes(List<byte[]> list) {
        List<byte[]> result = null;
        if (list != null) {
            result = new ArrayList<byte[]>(list.size());
            result.addAll(list);
            result.set(0, removeNamespace(result.get(0)));
        }
        return result;
    }
}
