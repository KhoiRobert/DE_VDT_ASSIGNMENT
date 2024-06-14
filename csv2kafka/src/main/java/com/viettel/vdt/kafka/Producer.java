package com.viettel.vdt.kafka;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class Producer {
    private static final String TOPIC_NAME = "log_action";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9094,localhost:9095";
    private static final String dataPath = "/home/khoi/Data/DE_VDT_ASSIGNMENT/data/log_action.csv";

    public static void main(String[] args) {

     
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props);
            BufferedReader reader = new BufferedReader(new FileReader(dataPath))) {
            String line;
            int recordCount = 0;

            while ((line = reader.readLine()) != null) {
                // Process the CSV line and create a ProducerRecord
                String[] fields = line.split(",");
                // String record = "{"student_code":"SV001","activity":"Study","numOfFile":5,"timestamp":1623844775040}";
//                String key = fields[0];  // Assuming the key is in the first column
                String key = getKey(fields); // Control partitions of records
                String record1 = "{\"student_code\":" + fields[0] + ",\"activity\":\"" + fields[1] + "\",\"numOfFile\":" + fields[2] + ",\"timestamp\":\"" + fields[3] + "\"}";
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, key, record1);

                // Send the record and handle the result with callback
                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (exception == null) {
                            System.out.println("Record id " + record.key() + " sent successfully - topic: " + metadata.topic() +
                                    ", partition: " + metadata.partition() +
                                    ", offset: " + metadata.offset());
                        } else {
                            System.err.println("Error while sending record " + record.key() + ": " + exception.getMessage());
                        }
                    }
                });

                recordCount++;
            }

            System.out.println(recordCount + " records sent to Kafka successfully.");
        } catch (IOException e) {
            System.out.println("Source not found or Can't connect to Kafka Broker");
        }
    }

    private static String getKey (String [] fields) {
        if (Integer.parseInt(fields[0]) % 3 == 0)
            return "selected";
        else return "other";
    }

    private static void sendBatch(KafkaProducer<String, String> producer, List<ProducerRecord<String, String>> batchRecords) {
        for (ProducerRecord<String, String> record : batchRecords) {
            producer.send(record);
        }
        producer.flush();
    }
}
