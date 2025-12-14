package com.example.kafka

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TestOutputTopic
import org.apache.kafka.streams.TopologyTestDriver
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.Properties

class UppercaseTopologyTest {

    private lateinit var testDriver: TopologyTestDriver
    private lateinit var input: TestInputTopic<String, String>
    private lateinit var output: TestOutputTopic<String, String>

    @BeforeEach
    fun setup() {
        val builder = StreamsBuilder()
        KafkaConfig().uppercaseTopology(builder)

        testDriver = TopologyTestDriver(
            builder.build(),
            Properties().apply {
                put(StreamsConfig.APPLICATION_ID_CONFIG, "test")
                put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234")
                put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().javaClass)
                put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().javaClass)
            }
        )

        input = testDriver.createInputTopic(
            "input-topic",
            StringSerializer(),
            StringSerializer()
        )

        output = testDriver.createOutputTopic(
            "output-topic",
            StringDeserializer(),
            StringDeserializer()
        )
    }

    @Test
    fun `uppercase works`() {
        input.pipeInput("k", "hello")
        assertEquals("HELLO", output.readValue())
    }
}