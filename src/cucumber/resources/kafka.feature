Feature: Kafka integration
  Scenario: Hello World kafka
    Given kafka cluster has kafka nodes "kafka-brokers-0.usw2.services.upgrade.com:32400,kafka-brokers-1.usw2.services.upgrade.com:32401,kafka-brokers-2.usw2.services.upgrade.com:32402"
    When a producer sends a message to "topic-test"
    Then a consumer receives a message from "topic-test" in group "group-test"
