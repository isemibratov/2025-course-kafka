package com.example.kafka

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.test.EmbeddedKafkaBroker
import org.springframework.kafka.test.context.EmbeddedKafka
import java.time.Duration

@SpringBootTest(
    properties = ["spring.kafka.bootstrap-servers=\${spring.embedded.kafka.brokers}"]
)
@EmbeddedKafka(
    partitions = 1,
    topics = ["input-topic", "output-topic"]
)
class KafkaStreamIntegrationTest {
    @Autowired
    private lateinit var embeddedKafka: EmbeddedKafkaBroker

    @Test
    fun `should convert message to uppercase`() {
        // Producer
        val producer = KafkaProducer<String, String>(
            mapOf(
                "bootstrap.servers" to embeddedKafka.brokersAsString,
                "key.serializer" to StringSerializer::class.java,
                "value.serializer" to StringSerializer::class.java
            )
        )
        producer.send(ProducerRecord("input-topic", "key", "hello kafka"))
        producer.flush()

        // Consumer
        val consumer = KafkaConsumer(
            mapOf(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to embeddedKafka.brokersAsString,
                ConsumerConfig.GROUP_ID_CONFIG to "test-group",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest"
            ),
            StringDeserializer(),
            StringDeserializer()
        )
        consumer.subscribe(listOf("output-topic"))

        val records = consumer.poll(Duration.ofSeconds(5))
        val value = records.first().value()

        assertEquals("HELLO KAFKA", value)
    }
}