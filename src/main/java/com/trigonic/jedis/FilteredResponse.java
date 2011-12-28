package com.trigonic.jedis;

import redis.clients.jedis.Response;

public abstract class FilteredResponse<T> extends Response<T> {
    private Response<T> wrappedResponse;
    
    public FilteredResponse(Response<T> wrappedResponse) {
        super(null);
        this.wrappedResponse = wrappedResponse;
    }
    
    protected abstract T filter(T value);
    
    @Override
    public T get() {
        return filter(wrappedResponse.get());
    }
}
