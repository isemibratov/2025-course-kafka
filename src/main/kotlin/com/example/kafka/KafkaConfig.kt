package com.example.kafka

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.Topology
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafkaStreams
import org.springframework.kafka.config.KafkaStreamsConfiguration
import kotlin.text.uppercase

@Configuration
@EnableKafkaStreams
class KafkaConfig {

    @Bean
    fun defaultKafkaStreamsConfig(
        @Value("\${spring.kafka.bootstrap-servers}") bootstrapServers: String
    ): KafkaStreamsConfiguration {
        val props = mapOf(
            StreamsConfig.APPLICATION_ID_CONFIG to "test-app",
            StreamsConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG to Serdes.String().javaClass,
            StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG to Serdes.String().javaClass
        )
        return KafkaStreamsConfiguration(props)
    }

    @Bean
    fun uppercaseTopology(builder: StreamsBuilder): Topology {
        builder.stream<String, String>("input-topic")
            .mapValues { value: String -> value.uppercase() }
            .to("output-topic")
        return builder.build()
    }
}