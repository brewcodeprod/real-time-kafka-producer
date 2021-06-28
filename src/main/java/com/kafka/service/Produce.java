package com.kafka.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.data.objects.Employee;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.FileNotFoundException;
import java.io.FileReader;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.*;

@Component
public class Produce {

    //@PostConstruct
    public void produceMessage() {
        //Creating Properties
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ProductSerializer.class);
        //Creating producers
        Producer<String, Employee> producer = new KafkaProducer<>(properties);
        /*ProducerRecord<String, List<Employee>> record = new
                ProducerRecord<String, List<Employee>>("product", UUID.randomUUID().toString(),
                products);
        producer.send(record);
        producer.flush();*/
        List<Employee> products = readJSON();
        for (Employee product : products) {
            System.out.println("Product: "+product);
            ProducerRecord<String, Employee> record = new ProducerRecord<String, Employee>("Employee", UUID.randomUUID().toString(),
                    product);
            //Sending message to Kafka Broker
            producer.send(record);
            //producer.flush();
        }
    }

    public List<Employee> readJSON() {
        JSONParser parser = new JSONParser();
        ObjectMapper objectMapper = new ObjectMapper();
        List<Employee> products = new ArrayList<>();
        try
            {
                Object object = parser.parse(new FileReader("src/main/resources/input-data/Employee.json"));
                JSONArray prodJsonArray = (JSONArray) object;
                Iterator<JSONObject> iterator = prodJsonArray.iterator();
                while (iterator.hasNext()) {
                Employee product = objectMapper.readValue(iterator.next().toJSONString(), Employee.class);
                System.out.println("Product: "+product);
                products.add(product);
                }
            }
        catch(FileNotFoundException fe)
        {
            fe.printStackTrace(); }
        catch(Exception e)
        {
            e.printStackTrace(); }
        return products;
    }
}
