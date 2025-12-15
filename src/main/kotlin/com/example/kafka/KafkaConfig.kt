package com.example.kafka

import com.example.avro.ProcessedEvent
import com.example.avro.UserEvent
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.avro.specific.SpecificRecord
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Produced
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafkaStreams
import org.springframework.kafka.config.KafkaStreamsConfiguration

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
        )
        return KafkaStreamsConfiguration(props)
    }

    @Bean
    fun uppercaseTopology(
        builder: StreamsBuilder,
        @Value("\${spring.kafka.properties.schema.registry.url:http://localhost:8081}")
        schemaRegistryUrl: String
    ): KStream<String, ProcessedEvent> = buildTopology(builder, schemaRegistryUrl)

    fun buildTopology(
        builder: StreamsBuilder,
        schemaRegistryUrl: String,
        schemaRegistryClient: SchemaRegistryClient? = null
    ): KStream<String, ProcessedEvent> {

        val keySerde = Serdes.String()
        val userEventSerde: Serde<UserEvent> = specificAvroSerde(schemaRegistryUrl, schemaRegistryClient)
        val processedSerde: Serde<ProcessedEvent> = specificAvroSerde(schemaRegistryUrl, schemaRegistryClient)

        val input: KStream<String, UserEvent> =
            builder.stream("input-topic", Consumed.with(keySerde, userEventSerde))

        val out: KStream<String, ProcessedEvent> =
            input
                .filter { _, v -> v.text.trim().isNotEmpty() }
                .mapValues { v ->
                    val upper = v.text.uppercase()
                    ProcessedEvent.newBuilder()
                        .setUserId(v.userId)
                        .setUpperText(upper)
                        .setLength(upper.length)
                        .setProcessedAt(System.currentTimeMillis())
                        .build()
                }

        out.to("output-topic", Produced.with(keySerde, processedSerde))
        return out
    }

    private fun <T : SpecificRecord> specificAvroSerde(
        schemaRegistryUrl: String,
        schemaRegistryClient: SchemaRegistryClient? = null,
        isKey: Boolean = false
    ): Serde<T> {
        val serde: SpecificAvroSerde<T> =
            if (schemaRegistryClient != null) SpecificAvroSerde(schemaRegistryClient)
            else SpecificAvroSerde()

        serde.configure(
            mapOf(
                AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG to schemaRegistryUrl,
                "specific.avro.reader" to "true"
            ),
            isKey
        )
        return serde
    }
}