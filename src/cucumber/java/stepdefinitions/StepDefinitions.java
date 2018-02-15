package stepdefinitions;

import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Properties;
import java.util.Collections;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Assert;

import cucumber.api.java.en.Given;
import cucumber.api.java.en.When;
import cucumber.api.java.en.Then;

import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;
import kafka.producer.KeyedMessage;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.consumer.KafkaStream;
import kafka.consumer.ConsumerIterator;
import kafka.message.MessageAndMetadata;

public class StepDefinitions {
  private String kafkaNodeList;
  private String zookeeperNodeList;

  @Given("^kafka cluster has kafka nodes \"([^\"]*)\" and zookeeper nodes \"([^\"]*)\"$")
  public void kafkaClusterHasNodesAndZookeeperNodes(String kafkaNodeList, String zookeeperNodeList) {
    this.kafkaNodeList = kafkaNodeList;
    this.zookeeperNodeList = zookeeperNodeList;
  }

  @Given("^kafka cluster has kafka nodes \"([^\"]*)\"")
  public void kafkaClusterHasNodesAndZookeeperNodes(String kafkaNodeList) {
    this.kafkaNodeList = kafkaNodeList;
  }

  @When("^a producer sends a message to \"([^\"]*)\"$")
  public void aProducerSendsAMessageTo(String topic) {
    Properties properties = new Properties();
    properties.setProperty("metadata.broker.list", kafkaNodeList);
    properties.setProperty("producer.type", "sync");
    properties.setProperty("request.required.acks", "1");

    ProducerConfig producerConfig = new ProducerConfig(properties);
    Producer<byte[], byte[]> producer = new Producer<>(producerConfig);

    String message = "FizzBuzz";
    byte[] messageBytes = message.getBytes();

    KeyedMessage<byte[], byte[]> keyedMessage = new KeyedMessage<>(topic, messageBytes);

    producer.send(keyedMessage);
  }

  @Then("^a consumer receives a message from \"([^\"]*)\" in group \"([^\"]*)\"$")
  public void aConsumerReceivesAMessageFromInGroup(String topic, String group) {
    Properties properties = new Properties();
    //properties.setProperty("zookeeper.connect", zookeeperNodeList);
    properties.setProperty("bootstrap.servers", kafkaNodeList);
    properties.setProperty("group.id", group);
    properties.setProperty("auto.offset.reset", "earliest");
    properties.setProperty("consumer.timeout.ms", "3000"); // ms
    properties.setProperty("key.deserializer", LongDeserializer.class.getName());
    properties.setProperty("value.deserializer", StringDeserializer.class.getName());


    Map<String, Integer> topicCountMap = new HashMap<>();
    topicCountMap.put(topic, 1);
    KafkaConsumer consumer = new KafkaConsumer(properties);

    consumer.subscribe(Collections.singletonList(topic));
    //Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreamsMap = consumer.createMessageStreams(topicCountMap);

/*    List<KafkaStream<byte[], byte[]>> consumerStreams = consumerStreamsMap.get(topic);
    KafkaStream<byte[], byte[]> consumerStream = consumerStreams.get(0);
    ConsumerIterator<byte[], byte[]> messageIterator = consumerStream.iterator();

    Assert.assertTrue(messageIterator.hasNext());

    MessageAndMetadata<byte[], byte[]> messageAndMetadata = messageIterator.next();*/

    final ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000);
    consumerRecords.forEach(record -> {
      System.out.printf("Consumer Record:(%d, %s, %d, %d)\n",
              record.key(), record.value(),
              record.partition(), record.offset());
    });

    consumer.commitAsync();
    consumer.close();

    //byte[] messageBytes = messageAndMetadata.message();

    //Assert.assertEquals("FizzBuzz", new String(messageBytes));
  }
}
