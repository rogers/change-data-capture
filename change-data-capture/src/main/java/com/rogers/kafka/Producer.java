package com.rogers.kafka;

/**
 * Recomended Kafka Settings
 * Producer: 
 * - acks = All. (To guarantee persistence)
 * - retries = Integer.MAX_VALUE  (infinit retries)
 * - max.block.ms = Integer.MAX_VALUE (indefinite blocking if brokers are down, or buffers are full)
 */
import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rogers.cdc.exceptions.ConfigException;

public class Producer implements Closeable {
	private Properties config;
	private boolean sync; // The Kafka Producer is async - force to send
							// messages asynchronously
	// private InputStream inputStream;
	private KafkaProducer<byte[], byte[]> producer;

	final private static Logger logger = LoggerFactory
			.getLogger(Producer.class);

	public Producer(Properties _config, boolean _sync) {

		logger.info("Kafka: Create KafkaProducerWrapper");
		config = _config;
		sync = _sync;
		try {
			producer = new KafkaProducer<byte[], byte[]>(config);
			Runtime.getRuntime().addShutdownHook(new Thread() {
				@Override
				public void run() {
					producer.close();
				}
			});

		} catch (Exception e) {
			logger.error("Failed to create KafkaProducer  " + e);
			// System.exit(1);
		}
		logger.info("Kafka: Done Create KafkaProducerWrapper");
	}

	public void send(final String topic, final byte[] key, final byte[] msg)
			throws InterruptedException, ExecutionException {
		ProducerRecord<byte[], byte[]> rec = new ProducerRecord<byte[], byte[]>(
				topic, key, msg);
		Future<RecordMetadata> res = producer.send(rec, new Callback() {
			@Override
			public void onCompletion(RecordMetadata recordMetadata, Exception e) {
				if (e != null) {
					logger.error(" failed to send record to {}: {}", topic, e);
					logger.debug("Failed record: topic {},  key {}, value {}",
							topic, key, msg);
				} else {
					logger.trace(
							"Wrote record successfully: topic {} partition {} offset {}",
							recordMetadata.topic(), recordMetadata.partition(),
							recordMetadata.offset());
				}
			}
		});
		if (sync) {
			res.get();
		}

	}

	public void send(final String topic, byte[] msg)
			throws InterruptedException, ExecutionException {
		ProducerRecord<byte[], byte[]> rec = new ProducerRecord<byte[], byte[]>(
				topic, msg);
		logger.debug("Sending message to Kafka topic {}", topic);
		producer.send(rec, new Callback() {
			@Override
			public void onCompletion(RecordMetadata recordMetadata, Exception e) {
				if (e != null) {
					// Given the default settings for zero data loss, this
					// should basically never happen --
					// between "infinite" retries, indefinite blocking on full
					// buffers, and "infinite" request
					// timeouts, callbacks with exceptions should never be
					// invoked in practice. If the
					// user overrode these settings, the best we can do is
					// notify them of the failure via
					// logging.
					logger.error("failed to send record to {}: {}", topic, e);
				} else {
					logger.debug(
							"Wrote record successfully: topic {} partition {} offset {}",
							recordMetadata.topic(), recordMetadata.partition(),
							recordMetadata.offset());
				}

			}
		});

	}

	@Override
	public void close() {
		logger.debug("closing producer: Metrics = ", producer.metrics());
		// metrics
		producer.close();

	}

}
