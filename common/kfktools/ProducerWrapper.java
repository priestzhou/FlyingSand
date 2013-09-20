package kfktools;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import kafka.producer.ProducerConfig;
import kafka.producer.KeyedMessage;
import kafka.javaapi.producer.Producer;

public class ProducerWrapper implements AutoCloseable {
    private Producer<byte[],byte[]> producer = null;

    public ProducerWrapper(Properties props) {
        ProducerConfig pc = new ProducerConfig(props);
        producer = new Producer<byte[],byte[]>(pc);
    }

    public void close() {
        producer.close();
    }

    public void send(List<KeyedMessage<byte[],byte[]>> msgs) {
        producer.send(msgs);
    }
}
