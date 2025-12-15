package com.example.kafka

import com.example.avro.ProcessedEvent
import com.example.avro.UserEvent
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.test.EmbeddedKafkaBroker
import org.springframework.kafka.test.context.EmbeddedKafka
import java.time.Duration

@SpringBootTest(
    properties = [
        "spring.kafka.bootstrap-servers=\${spring.embedded.kafka.brokers}",
        "spring.kafka.properties.${AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG}=mock://test",
        "spring.kafka.properties.specific.avro.reader=true"
    ]
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
        val scope = "test"
        val schemaRegistryUrl = "mock://$scope"

        val schemaClient = MockSchemaRegistry.getClientForScope(scope)

        val producer = KafkaProducer<String, Any>(
            mapOf(
                "bootstrap.servers" to embeddedKafka.brokersAsString,
                "key.serializer" to StringSerializer::class.java,
                "value.serializer" to KafkaAvroSerializer::class.java,
                AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG to schemaRegistryUrl
            )
        )

        producer.use {
            val event = UserEvent.newBuilder()
                .setUserId("u1")
                .setText("hello kafka")
                .setEventTs(1L)
                .build()

            it.send(ProducerRecord("input-topic", "key", event))
            it.flush()
        }

        val consumerProps = mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to embeddedKafka.brokersAsString,
            ConsumerConfig.GROUP_ID_CONFIG to "test-group",
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
            AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG to schemaRegistryUrl,
            "specific.avro.reader" to true
        )

        val valueDeserializer = KafkaAvroDeserializer(schemaClient).apply {
            configure(consumerProps, false)
        } as Deserializer<ProcessedEvent>

        val consumer = KafkaConsumer(
            consumerProps,
            StringDeserializer(),
            valueDeserializer
        )

        consumer.use {
            it.subscribe(listOf("output-topic"))

            val deadline = System.currentTimeMillis() + 10_000
            var last: ProcessedEvent? = null

            while (System.currentTimeMillis() < deadline && last == null) {
                val records = it.poll(Duration.ofMillis(250))
                val rec = records.firstOrNull()
                if (rec != null) last = rec.value()
            }

            assertTrue(last != null, "No records received from output-topic within timeout")

            val value = last!!
            assertEquals("u1", value.userId.toString())
            assertEquals("HELLO KAFKA", value.upperText.toString())
            assertEquals(11, value.length)
        }
    }
}