package com.ibm.kafkastream.wordcount.config;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "kafka-word-count")
public class WordCountKafkaConfiguration {
	private static final Logger logger = Logger.getLogger(WordCountKafkaConfiguration.class.getName());
	public static final long PRODUCER_TIMEOUT_SECS = 10;
	public static final long PRODUCER_CLOSE_TIMEOUT_SEC = 10;
	public static final Duration CONSUMER_POLL_TIMEOUT = Duration.ofSeconds(10);
	public static final Duration CONSUMER_CLOSE_TIMEOUT = Duration.ofSeconds(10);
    public static final long TERMINATION_TIMEOUT_SEC = 10;

    private final Map<String, String> commonProperties = new HashMap<>();
	public Map<String, String> getCommonProperties(){
		return this.commonProperties;
	}

	private final Map<String, String> producerProperties = new HashMap<>();
	public Map<String, String> getProducerProperties(){
		return this.producerProperties;
	}

	private final Map<String, String> consumerProperties = new HashMap<>();
	public Map<String, String> getConsumerProperties(){
		return this.consumerProperties;
	}
    
    /**
     * Take into account the environment variables if set
     *
     * @return common kafka properties
     */
    private  Properties buildCommonProperties() {
        Properties properties = new Properties();
        Map<String, String> env = System.getenv();
        logger.info(String.format("Bootstrap Servers: %s", commonProperties.get("bootstrapServers")));
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, commonProperties.get("bootstrapServers"));

    	if (!commonProperties.get("apiKey").isEmpty()) {
          properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, commonProperties.get("securityProtocol"));
          properties.put(SaslConfigs.SASL_MECHANISM, commonProperties.get("saslMechanism"));
          properties.put(SaslConfigs.SASL_JAAS_CONFIG,
                    String.format("org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
							commonProperties.get("username"),
							commonProperties.get("apiKey")));
          properties.put(SslConfigs.SSL_PROTOCOL_CONFIG, commonProperties.get("sslProtocol"));
          properties.put(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, commonProperties.get("sslEnabledProtocols"));
          properties.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, commonProperties.get("sslEndpointIdentificationAlgorithm"));

          if ("true".equals(commonProperties.get("truststoreEnabled"))){
            properties.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, commonProperties.get("sslTruststoreLocation"));
            properties.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, commonProperties.get("sslTruststorePassword"));
          }
        }
        return properties;
    }

	public String getWordCountProducerTopicName() {
		return producerProperties.get("topic");
	}

	
	public String getConsumerGroupID() {
		return consumerProperties.get("groupId");
	}


	public Properties getProducerProperties(String clientId) {
		Properties properties = buildCommonProperties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        properties.put(ProducerConfig.ACKS_CONFIG, producerProperties.get("acks"));
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, producerProperties.get("enableIdempotence"));
        properties.forEach((k,v)  -> logger.info(k + " : " + v)); 
        return properties;
	}

}
