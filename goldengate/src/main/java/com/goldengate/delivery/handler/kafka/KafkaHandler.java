/*
 *
 * Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved.
 *
 */
package com.goldengate.delivery.handler.kafka;

import java.io.IOException;

import com.rogers.goldengate.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.goldengate.atg.datasource.DsConfiguration;
import com.goldengate.atg.datasource.adapt.Op;
import com.goldengate.atg.datasource.adapt.Tx;
import com.goldengate.atg.datasource.meta.DsMetaData;
import com.goldengate.atg.datasource.meta.TableMetaData;
import com.goldengate.atg.datasource.meta.TableName;
import com.rogers.goldengate.mutationmappers.TypedMutationMapper;
import com.rogers.cdc.api.mutations.MutationMapper;
import com.rogers.cdc.handlers.Handler;
import com.rogers.cdc.handlers.KafkaAvroHandler;

//TODO: Fix the desc
/**
 * KafkaHandler is an extension of GoldenGate Java Adapters - "EventHandlers".
 * It operates on the column values received from the operations, creates Kafka
 * messages out of the column values and publishes the events to Kafka when the
 * transaction commit occurs.
 * 
 * In "tx" mode, the events will be buffered till the transaction commit is
 * received and all the events will be published in batch, once the transaction
 * is committed. In "op" mode, the events will be published on every
 * operation/record basis.
 * 
 * The Kafka Java Client (as of Kafka 0.8.2) is asynchronous and support message
 * batching/buffering. As such, the "tx" and "op" modes should have comparable
 * performance.
 * 
 * 
 * Considering a table "TCUST" with columns "CUST_ID", "CUST_NAME", "ADDRESS"
 * with a record being inserted into it say "10001","Kafka Admin","Los Angles".
 * The final data published into Flume would be similar to the following.
 * (Assuming "," as the configured delimiter) ### OperationType,Col-1, Col-2,
 * Col-3, Operation Timestamp ### I,10001,Kafka Admin,Los Angles,2014-12-18
 * 08:28:02.000000
 * 
 * TODO: Do we care about the bellow?
 * The Operation Type and Operation Timestamp are configurable. By default both
 * Operation Type and Operation Timestamp will be part of the delimited
 * separated values.
 * 
 * @author Eugene Miretsky
 * 
 * */

public class KafkaHandler extends CleanAbstractHandler {
	final private static Logger logger = LoggerFactory
			.getLogger(KafkaHandler.class);

	// TODO: Move it to a test class

	/**
	 * Config file for Kafka Producer
	 * http://kafka.apache.org/082/javadoc/org/apache
	 * /kafka/clients/producer/KafkaProducer.html
	 */
	// TODO: Should be generic...for Kafka/HDFS/etc
	private String kafkaConfigFile;
	private final String KAFKA_CONFIG_FILE = "kafka.properties";

	/**
	 * Collection to store kafka messages until the "transaction commit" event
	 * is received in "tx" mode, upon which the list will be cleared
	 */
	// private List<ProducerRecordWrapper> events = new
	// ArrayList<ProducerRecordWrapper>();
	private com.rogers.goldengate.HandlerProperties handlerProperties;

	private Handler<Op, TableMetaData, MutationMapper<Op, TableMetaData>> handler;

	@Override
	public void init(DsConfiguration arg0, DsMetaData arg1) {
		super.init(arg0, arg1);
		// TODO: Do something with the config file
		kafkaConfigFile = KAFKA_CONFIG_FILE; // set default value
		MutationMapper<Op, TableMetaData> mapper = new TypedMutationMapper();// TODO:
																				// get
																				// this
																				// from
																				// a
																				// config
																				// file
		handler = new KafkaAvroHandler<Op, TableMetaData, MutationMapper<Op, TableMetaData>>(
				mapper, kafkaConfigFile);
		super.init(arg0, arg1);
		logger.info("Done Initializing Kafka Handler");
	}

	@Override
	public void destroy() {

		logger.info("Destroy event");
		try {
			handler.close();
		} catch (IOException e) {

			logger.error("Failed to close handler properly ", e);
		}

		super.destroy();
	}
	protected void processOpImpl(Tx currentTx, Op op) {

		TableName tname = op.getTableName();
		//TableMetaData tMeta = getMetaData().getTableMetaData(tname);
		handler.processOp(op);

	}

	public String getKafkaConfigFile() {
		return kafkaConfigFile;
	}

	public void setKafkaConfigFile(String delimiter) {
		this.kafkaConfigFile = delimiter;
	}


	

}
