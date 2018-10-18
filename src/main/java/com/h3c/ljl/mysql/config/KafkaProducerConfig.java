package com.h3c.ljl.mysql.config;

import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
public class KafkaProducerConfig {
    @Value("${kafka.producer.servers}")
    private String servers;
    @Value("${kafka.producer.retries}")
    private int retries;
    @Value("${kafka.producer.batch.size}")
    private int batchSize;
    @Value("${kafka.producer.linger}")
    private int linger;
    @Value("${kafka.producer.buffer.memory}")
    private int bufferMemory;

    public Map<String, Object> producerConfigs() {
        Map<String, Object> props = new HashMap<>();
        String kafkaServers = System.getenv("KAFKA_SERVERS");
        if(kafkaServers != null){
            this.servers = kafkaServers;
        }
        props.put(org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        props.put(org.apache.kafka.clients.producer.ProducerConfig.RETRIES_CONFIG, retries);
        props.put(org.apache.kafka.clients.producer.ProducerConfig.BATCH_SIZE_CONFIG, batchSize);
        props.put(org.apache.kafka.clients.producer.ProducerConfig.LINGER_MS_CONFIG, linger);
        props.put(org.apache.kafka.clients.producer.ProducerConfig.BUFFER_MEMORY_CONFIG, bufferMemory);
        props.put(org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        // props.put("partitioner.class", "com.h3c.test.ljl.util.Partition");
        return props;
    }

    public ProducerFactory<String, String> producerFactory() {
        return new DefaultKafkaProducerFactory<>(producerConfigs());
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<String, String>(producerFactory());
    }


     /*public static void sendMsg(String topic, String sendKey, String data){
        Producer producer = createProducer();
        producer.send(new KeyedMessage<String, String>(topic,sendKey,data));
        //producer.send(new KeyedMessage<String, String>("canal","123","guofei"));
        System.out.println("sendKey:"+sendKey);
    }
    public static void main(String args[]) {
        Producer producer = createProducer();
        //producer.send(new KeyedMessage<String, String>(topic,sendKey,data));


        for (int i=1;i<=5;i++){
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            producer.send(new KeyedMessage<String, String>("canal","123"+i,"guofei\t"+i));
        }

    }

    public static Producer<Integer,String> createProducer(){
        Properties properties = new Properties();

        properties.put("zookeeper.connect", "172.27.8.15:2181");
        properties.put("serializer.class", StringEncoder.class.getName());
        properties.put("metadata.broker.list", "172.27.8.15:9092");// 声明kafka broker


        return new Producer<Integer,String>(new ProducerConfig(properties));
    }*/

}
