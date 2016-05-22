package com.rogers.goldengate;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//TODO: Move the whole GoldenGate part to a different package - this package should not depend on Oracle
public class KafkaProducerConfig {
	Properties prop;
	InputStream inputStream;
	String propFileName = "kafka.properties";

	final private static Logger logger = LoggerFactory
			.getLogger(KafkaProducerConfig.class);

	// TODO dealing with exceptions in Const is probably not a good idea.
	public KafkaProducerConfig() throws IOException {
		prop = new Properties();

		try {
			inputStream = getClass().getClassLoader().getResourceAsStream(
					propFileName);
			if (inputStream != null) {
				prop.load(inputStream);
			} else {
				throw new FileNotFoundException("property file '"
						+ propFileName + "' not found in the classpath");
			}
		} catch (Exception e) {
			logger.error("Failed to open Kafka Producer config{}", e);
		} finally {
			inputStream.close();
		}
	}

	public String getPropValue(String key) throws IOException {
		return prop.getProperty(key);
	}
}
