package master2015;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

/**
 *
 * @author yolanda
 */
public class KafkaConsumer extends BaseRichSpout {

    private static final long serialVersionUID = 1L;
    private ConsumerConnector consumer;
    private SpoutOutputCollector collector;
    String a_zookeeper;
    String a_groupId;
    private static final String KAFKA_TOPIC = "twitter-topic";

    public KafkaConsumer(String a_zookeeper, String a_groupId) {
        System.out.println("-------------------------");
        this.a_zookeeper = a_zookeeper;
        this.a_groupId = a_groupId;
        System.out.println("-------------------------");
    }

    public void declareOutputFields(OutputFieldsDeclarer ofd) {
        ofd.declare(new Fields("tweet"));
    }

    @Override
    public void ack(Object id) {
    }

    @Override
    public void fail(Object id) {
    }

    private kafka.consumer.ConsumerConfig createConsumerConfig(String a_zookeeper, String a_groupId) {
        Properties props = new Properties();
        props.put("zookeeper.connect", a_zookeeper);
        props.put("group.id", a_groupId);
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        return new ConsumerConfig(props);
    }

    public void open(Map map, TopologyContext tc, SpoutOutputCollector soc) {
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig(a_zookeeper, a_groupId));
        this.collector = soc;
    }

    public void nextTuple() {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(KAFKA_TOPIC, 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap
                = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(KAFKA_TOPIC);
  
        ConsumerIterator<byte[], byte[]> it = streams.get(0).iterator();
        while (it.hasNext()) {
            System.out.println("Received:" + new String(it.next().message()));

            collector.emit( new Values(new String(it.next().message())));
        }
    }

}
