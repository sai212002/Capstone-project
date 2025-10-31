package com.capstone.sender;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import com.capstone.domain.Account;
import com.capstone.serializer.AccountSerializer;

public class AccountSender {
        public static void main(String[] args) {
                Properties props = new Properties();
                props.put("bootstrap.servers", "localhost:9092");
                props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
                props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AccountSerializer.class);
                props.put("partitioner.class", "com.capstone.receiver.AccountTypePartitioner");
                KafkaProducer<String, Account> producer = new KafkaProducer<>(props);
                String topic = "account-topic";

                for (int i = 1001; i <= 1010; i++) {
                        int cust = 23;
                        Account account = new Account(i + cust, i, "CA", "Hitech-city");
                        ProducerRecord<String, Account> record = new ProducerRecord<>(topic, "CA", account);
                        producer.send(record);
                }
                for (int i = 2001; i <= 2010; i++) {
                        int cust = 56;
                        Account account = new Account(i + cust, i, "SB", "MindSpace");
                        ProducerRecord<String, Account> record = new ProducerRecord<>(topic, "SB", account);
                        producer.send(record);
                }

                for (int i = 3001; i <= 3010; i++) {
                        int cust = 67;
                        Account account = new Account(i + cust, i, "RD", "Balanagar");
                        ProducerRecord<String, Account> record = new ProducerRecord<>(topic, "RD", account);
                        producer.send(record);
                }
                for (int i = 4001; i <= 4010; i++) {
                        int cust = 78;
                        Account account = new Account(i + cust, i, "Loan", "Chintal");
                        ProducerRecord<String, Account> record = new ProducerRecord<>(topic, "Loan", account);
                        producer.send(record);
                }
                producer.close();
                System.out.println("Messages sent");

        }
}