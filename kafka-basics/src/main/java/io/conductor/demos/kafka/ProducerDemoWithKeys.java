package io.conductor.demos.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithKeys {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithKeys.class.getSimpleName());
    public static void main(String[] args) {
        log.info("I'm a kafka producer!");

        //create producer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers" , "127.0.0.1:9092");

        //set producer properties
        properties.setProperty("key.serializer" , StringSerializer.class.getName());
        properties.setProperty("value.serializer" , StringSerializer.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        for (int j = 0 ; j < 2 ; j++){
            for (int i = 0; i < 30 ; i++){

                String topic = "demo_java2";
                String key = "id_" + i;
                String value = "hello world " + i;
                //create a producer record
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);
                //send data
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        //executes everytime a record successfully sent or an exception is thrown
                        if (e == null){
                            //the record was successfully sent
                            log.info("Key : " + key + "\n"
                                    + "partition : " + metadata.partition() + "\n");
                        } else {
                            log.error("error while producing", e);
                        }
                    }
                });

            }
        }


        //tell the producer to send all data and block until done - synchronous
        producer.flush();
        //flush and close the producer
        producer.close();
    }
}
