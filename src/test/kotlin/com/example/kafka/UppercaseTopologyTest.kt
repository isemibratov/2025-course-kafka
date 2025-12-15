package com.example.kafka

import com.example.avro.ProcessedEvent
import com.example.avro.UserEvent
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TestOutputTopic
import org.apache.kafka.streams.TopologyTestDriver
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.Properties

class UppercaseTopologyTest {
    private lateinit var testDriver: TopologyTestDriver
    private lateinit var input: TestInputTopic<String, UserEvent>
    private lateinit var output: TestOutputTopic<String, ProcessedEvent>

    private val schemaClient = MockSchemaRegistryClient()

    @BeforeEach
    fun setup() {
        val builder = StreamsBuilder()

        KafkaConfig().buildTopology(builder, "mock://test", schemaClient)

        val keySerde = Serdes.String()
        val serdeConfig = mapOf(
            AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG to "mock://test",
            "specific.avro.reader" to "true"
        )

        val userEventSerde: Serde<UserEvent> = SpecificAvroSerde<UserEvent>(schemaClient).apply {
            configure(serdeConfig, false)
        }
        val processedSerde: Serde<ProcessedEvent> = SpecificAvroSerde<ProcessedEvent>(schemaClient).apply {
            configure(serdeConfig, false)
        }

        testDriver = TopologyTestDriver(
            builder.build(),
            Properties().apply {
                put(StreamsConfig.APPLICATION_ID_CONFIG, "test")
                put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234")
                put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://test")
            }
        )

        input = testDriver.createInputTopic("input-topic", keySerde.serializer(), userEventSerde.serializer())
        output = testDriver.createOutputTopic("output-topic", keySerde.deserializer(), processedSerde.deserializer())
    }

    @AfterEach
    fun tearDown() {
        testDriver.close()
    }

    @Test
    fun `uppercase works`() {
        input.pipeInput(
            "k1",
            UserEvent.newBuilder().setUserId("u1").setText("hello").setEventTs(1L).build()
        )

        val out1 = output.readValue()
        assertEquals("u1", out1.userId.toString())
        assertEquals("HELLO", out1.upperText.toString())
        assertEquals(5, out1.length)

        input.pipeInput(
            "k2",
            UserEvent.newBuilder().setUserId("u1").setText("kafka").setEventTs(2L).build()
        )

        val out2 = output.readValue()
        assertEquals("KAFKA", out2.upperText.toString())
        assertEquals(5, out2.length)
    }
}