package com.periscope.kafka;

import com.periscope.config.KafkaConfig;
import com.periscope.model.ChangeEvent;

public class TopicRouter {

	private final String topicPrefix;

	public TopicRouter(KafkaConfig config) {
		this.topicPrefix = config.topicPrefix();
	}

	/**
	 * A simple record to hold the routing decision.
	 * Equivalent to returning two strings in Go: (topic, key)
	 */
	public record RoutingDecision(String topic, String key) {}

	public RoutingDecision route(ChangeEvent event) {
		// Format should be: prefix.schemaName.tableName
		String topic = String.format(
			"%s.%s.%s",
			topicPrefix,
			event.schemaName(),
			event.tableName()
		);

		// If it is null, just leave the key as null (Kafka will handle it).
		String key = null;
		if (event.getPrimaryKeyValue() != null) {
			key = event.getPrimaryKeyValue().toString();
		}

		return new RoutingDecision(topic, key);
	}
}
