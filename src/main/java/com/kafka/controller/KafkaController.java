package com.kafka.controller;

import com.kafka.service.Produce;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;

@RestController
public class KafkaController {

    @Autowired
    private Produce produce;

    public KafkaController(Produce produce) {
        this.produce = produce;
    }

    @RequestMapping(value="/products", method = RequestMethod.POST)
    private void fireRules(@RequestBody String topic) throws IOException {
        System.out.println("Sending messages to kafka topic: "+topic);
        this.produce.produceMessage();
    }
}
