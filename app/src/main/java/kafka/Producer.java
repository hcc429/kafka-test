package kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class Producer {

    private KafkaProducer<String, String> producer;
    Producer(String bootstrapServer){
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServer);

        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        this.producer = new KafkaProducer<>(props);
    }
    void send(String topic, String key, String message){
        this.producer.send(new ProducerRecord<>(topic, key, message));
    }

    void close(){
        this.producer.close();
    }
}
