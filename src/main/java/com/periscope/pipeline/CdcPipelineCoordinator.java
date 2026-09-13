package com.periscope.pipeline;

import com.periscope.cdc.CdcStreamConsumer;
import com.periscope.cdc.WalMessageParser;
import com.periscope.kafka.KafkaChangePublisher;
import com.periscope.kafka.TopicRouter;
import java.sql.SQLException;
import java.util.concurrent.CompletableFuture;

public class CdcPipelineCoordinator implements AutoCloseable {

	private final CdcStreamConsumer consumer;
	private final WalMessageParser parser;
	private final TopicRouter router;
	private final KafkaChangePublisher publisher;

	public CdcPipelineCoordinator(
		CdcStreamConsumer consumer,
		WalMessageParser parser,
		TopicRouter router,
		KafkaChangePublisher publisher
	) {
		this.consumer = consumer;
		this.parser = parser;
		this.router = router;
		this.publisher = publisher;
	}

	public void start() throws java.sql.SQLException {
		// Inside the lambda:
		consumer.start(event -> {
			TopicRouter.RoutingDecision decision = router.route(event);
			CompletableFuture<Void> future = publisher.publish(
				decision.topic(),
				decision.key(),
				event
			);

			future.thenAccept(v -> {
				try {
					consumer.acknowledgeLsn(event.lsn());
				} catch (SQLException e) {
					e.printStackTrace();
				}
			});
		});
	}

	@Override
	public void close() throws Exception {
		consumer.close();
		publisher.close();
	}
}
