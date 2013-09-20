package kfktools;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.Message;
import kafka.consumer.Consumer;

public class ConsumerWrapper implements AutoCloseable {
    private ConsumerConnector connector = null;

    public ConsumerWrapper(Properties prop) {
        connector = Consumer.createJavaConsumerConnector(new ConsumerConfig(prop));
    }

    public ConsumerIterator<byte[], byte[]> listenTo(String topic) {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(1));
        Map<String, List<KafkaStream<byte[], byte[]>>> streamMap = 
            connector.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream =  streamMap.get(topic).get(0);
        return stream.iterator();
    }

    public void close() {
        connector.shutdown();
    }
}
