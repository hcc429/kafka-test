package kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;
public class Consumer {

    private KafkaConsumer<String, String> consumer;
    Consumer(String bootstrapServer){
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServer);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest");
        props.put("group.id", "test");
        consumer = new KafkaConsumer<>(props);
    }

    void subscribe(String topic) {
        this.consumer.subscribe(Arrays.asList(topic));
    }

    void consumeAndPrint(){
        int max = 10;
        while(max-- > 0) {
            ConsumerRecords<String, String> records = this.consumer.poll(1000);
            for(ConsumerRecord<String, String> record: records) {
                System.out.printf("key: %s, value: %s\n", record.key(), record.value());
            }
        }
    }
}
