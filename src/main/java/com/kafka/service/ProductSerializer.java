package com.kafka.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.test.importproducts.ImportProduct;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.common.serialization.Serializer;

import java.io.Serializable;
import java.util.Map;

public class ProductSerializer<T extends Serializable> implements Serializer<T> {

    public ProductSerializer() {}
    @Override public void configure(Map<String, ?> map, boolean b) {
    }

    @Override
    public byte[] serialize(String topic, T data) {
        return SerializationUtils.serialize(data);
    }

    @Override public void close() {
    }
}
