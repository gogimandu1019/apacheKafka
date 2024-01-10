package io.conductor.demos.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());
    public static void main(String[] args) {
        log.info("I'm a kafka producer!");

        //create producer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers" , "127.0.0.1:9092");

        //set producer properties
        properties.setProperty("key.serializer" , StringSerializer.class.getName());
        properties.setProperty("value.serializer" , StringSerializer.class.getName());
        properties.setProperty("batch.size", "400" );
       // properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());
        //create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int j = 0; j < 10; j++){
            for (int i = 0; i < 30 ; i++){
                //create a producer record
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java2", "hello world" + i);
                //send data
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        //executes everytime a record successfully sent or an exception is thrown
                        if (e == null){
                            //the record was successfully sent
                            log.info("received new metadata \n"
                                    + "topic : " + metadata.topic() + "\n"
                                    + "partition : " + metadata.partition() + "\n"
                                    + "offset : " + metadata.offset() + "\n"
                                    + "timestamp : " + metadata.timestamp());
                        } else {
                            log.error("error while producing", e);
                        }
                    }
                });


            }
            try{
                Thread.sleep(500);
            }catch (InterruptedException e){
                e.printStackTrace();
            }
        }

        //tell the producer to send all data and block until done - synchronous
        producer.flush();
        //flush and close the producer
        producer.close();
    }
}
