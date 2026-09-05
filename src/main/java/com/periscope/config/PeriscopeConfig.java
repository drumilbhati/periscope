package com.periscope.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public record PeriscopeConfig(DatabaseConfig databaseConfig, KafkaConfig kafkaConfig) {
    public static PeriscopeConfig load() throws IOException {
        Properties props = new Properties();
        try (InputStream is = PeriscopeConfig.class.getClassLoader().getResourceAsStream("application.properties")) {
            if (is == null) {
                throw new IllegalStateException("application.properties not found on classpath");
            }
            props.load(is);
        }
        return load(props);
    }

    public static PeriscopeConfig load(Properties props) throws IOException {
        String dbHost = getSetting(props, "periscope.db.host", "PERISCOPE_DB_HOST", "localhost");
        int dbPort = Integer.parseInt(getSetting(props, "periscope.db.port", "PERISCOPE_DB_PORT", "5432"));
        String dbName = getSetting(props, "periscope.db.name", "PERISCOPE_DB_NAME", "periscope_db");
        String dbUser = getSetting(props, "periscope.db.user", "PERISCOPE_DB_USER", "postgres");
        String dbPassword = getSetting(props, "periscope.db.password", "PERISCOPE_DB_PASSWORD", "periscope");
        String dbSlot = getSetting(props, "periscope.db.slot", "PERISCOPE_DB_SLOT", "periscope_slot");
        String dbPub = getSetting(props, "periscope.db.publication", "PERISCOPE_DB_PUBLICATION", "periscope_pub");

        DatabaseConfig dbConfig = new DatabaseConfig(dbHost, dbPort, dbName, dbUser, dbPassword, dbSlot, dbPub);

        String kafkaBootstrap = getSetting(props, "periscope.kafka.bootstrap-servers", "PERISCOPE_KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        String kafkaAcks = getSetting(props, "periscope.kafka.acks", "PERISCOPE_KAFKA_ACKS", "all");
        int kafkaRetries = Integer.parseInt(getSetting(props, "periscope.kafka.retries", "PERISCOPE_KAFKA_RETRIES", "3"));
        String kafkaPrefix = getSetting(props, "periscope.kafka.topic-prefix", "PERISCOPE_KAFKA_TOPIC_PREFIX", "periscope");

        KafkaConfig kafkaConfig = new KafkaConfig(kafkaBootstrap, kafkaAcks, kafkaRetries, kafkaPrefix);

        return  new PeriscopeConfig(dbConfig, kafkaConfig);
    }

    private static String getSetting(Properties props, String propKey, String envKey, String defaultValue) {
        String envVal = System.getenv(envKey);
        if (envVal != null && !envVal.isBlank()) {
            return envVal;
        }
        return props.getProperty(propKey, defaultValue);
    }
}
