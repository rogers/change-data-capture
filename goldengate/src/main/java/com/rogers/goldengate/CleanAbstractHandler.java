package com.rogers.goldengate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.goldengate.atg.datasource.AbstractHandler;
import com.goldengate.atg.datasource.DsConfiguration;
import com.goldengate.atg.datasource.DsEvent;
import com.goldengate.atg.datasource.DsOperation;
import com.goldengate.atg.datasource.DsTransaction;
import com.goldengate.atg.datasource.GGDataSource.Status;
import com.goldengate.atg.datasource.adapt.Op;
import com.goldengate.atg.datasource.adapt.Tx;
import com.goldengate.atg.datasource.meta.DsMetaData;
import com.goldengate.atg.datasource.meta.TableMetaData;

/**
 * A basic implementation of GoldenGate Java Adapter AbstractHandler GoldenGate
 * comes with a few adapter examples (HFDS, Flume, JMX, FlatFile, etc). Each
 * seems to re-implement AbstractHandler in a somewhat different way. Considering GG 
 * is hard to debug as is, this leads to a lot of bugs.
 * This class provides a basic implementation that can be
 * shared across adapters
 * 
 * To implement an adapter, extend this class and implement a processOpImpl method
 * 
 *  * In "tx" mode, the events will be buffered till the transaction commit is
 * received and all the events will be published in batch, once the transaction
 * is committed. In "op" mode, the events will be published on every
 * operation/record basis.

 * 
 * @author Eugene Miretsky
 * 
 * */

//TODO: Fix logging to use param list

public abstract class CleanAbstractHandler extends AbstractHandler {
	final private static Logger logger = LoggerFactory
			.getLogger(CleanAbstractHandler.class);

	private HandlerProperties handlerProperties;

	/**
	 * Indicates if the operation timestamp should be included as part of output
	 * in the delimited separated values true - Operation timestamp will be
	 * included in the output false - Operation timestamp will not be included
	 * in the output
	 **/
	protected Boolean includeOpTimestamp = true;

	abstract protected void processOpImpl(Tx currentTx, Op op);

	@Override
	public void init(DsConfiguration arg0, DsMetaData arg1) {
		// TODO: Do something with the config file
		super.init(arg0, arg1);
		initializeHandlerProperties();
	}

	private void initializeHandlerProperties() {
		// TODO: Has to be called somehwere!!!!
		// Properties are not working now. Just a mock
		this.handlerProperties = new HandlerProperties();
		this.handlerProperties.includeOpTimestamp = this.includeOpTimestamp;
	}

	@Override
	final public Status metaDataChanged(DsEvent e, DsMetaData meta) {
		logger.info("Metadata change event");
		return super.metaDataChanged(e, meta);
	}

	@Override
	final public Status operationAdded(DsEvent e, DsTransaction transaction,
			DsOperation operation) {

		logger.info("Operation added event. Operation type = "
				+ operation.getOperationType());
		Status status = Status.OK;
		super.operationAdded(e, transaction, operation);

		if (isOperationMode()) {
			final Tx tx = new Tx(transaction, getMetaData(), getConfig());
			final TableMetaData tMeta = getMetaData().getTableMetaData(
					operation.getTableName());
			final Op op = new Op(operation, tMeta, getConfig());

			status = processOp(tx, op); // process data...

			// TODO: Should we flush somewhere here?
		}

		return status;

	}

	final protected Status processOp(Tx currentTx, Op op) {

		Status status = Status.OK;
		logger.debug("Process operation: table=[" + op.getTableName() + "]"
				+ ", op pos=" + op.getPosition() + ", tx pos="
				+ currentTx.getTranID() + ", op ts=" + op.getTimestamp());

		try {
			processOpImpl(currentTx, op);
			handlerProperties.totalOperations++;
		} catch (RuntimeException e) {
			status = Status.ABEND;
			logger.error(
					"Failed to Process operation: table=[" + op.getTableName()
							+ "]" + ", op pos=" + op.getPosition()
							+ ", tx pos=" + currentTx.getTranID() + ", op ts="
							+ op.getTimestamp() + " with error: ", e);
		}
		return status;

	}

	@Override
	final public String reportStatus() {
		logger.info("Reporting Status ");

		StringBuilder sb = new StringBuilder();

		sb.append("Status report: mode=").append(getMode());
		sb.append(", transactions=").append(handlerProperties.totalTxns);
		sb.append(", operations=").append(handlerProperties.totalOperations);
		/*
		 * TODO: operation types are not tracked right now
		 * sb.append(", inserts=").append(handlerProperties.totalInserts);
		 * sb.append(", updates=").append(handlerProperties.totalUpdates);
		 * sb.append(", deletes=").append(handlerProperties.totalDeletes);
		 */

		logger.info("Final Status " + sb.toString());

		return sb.toString();
	}

	@Override
	final public Status transactionBegin(DsEvent e, DsTransaction tx) {
		logger.info("Transaction begin event");
		return super.transactionBegin(e, tx);
	}

	@Override
	final public Status transactionCommit(DsEvent e, DsTransaction transaction) {
		logger.info("Transaction commit event ");
		super.transactionCommit(e, transaction);
		Status status = Status.OK;

		// Increment the number of transactions
		handlerProperties.totalTxns++;

		Tx tx = new Tx(transaction, getMetaData(), getConfig());

		// In 'operation mode', all the operations would have been processed
		// when
		// 'operationAdded' is called. In 'transaction mode', they are processed
		// when the commit event is received.
		if (!isOperationMode()) {
			for (Op op : tx) {
				status = processOp(tx, op); // process data...
				if (status != Status.OK) {
					// Break out of this loop
					break;
				}
			}
		}
		if (status == Status.OK) {
			logger.debug("Calling flushfor transaction commit pos="
					+ tx.getTranID());
			// Transaction is complete. Flush the data. Probably safer to hsync
			// but the
			// performance is terrible.
			try {
				// TODO: do we need this?
				// handler.flush();
			} catch (RuntimeException err) {
				status = Status.ABEND;
				logger.error("Failed to Process transaction {} with error: {}",
						tx, err);
			}
		}

		logger.debug("  Received transaction commit event, transaction count="
				+ handlerProperties.totalTxns + ", pos=" + tx.getTranID()
				+ " (total_ops= " + tx.getTotalOps() + ", buffered="
				+ tx.getSize() + ")" + ", ts=" + tx.getTimestamp());

		return status;

	}

	public HandlerProperties getHandlerProperties() {
		return handlerProperties;
	}

	public void setHandlerProperties(HandlerProperties handlerProperties) {
		this.handlerProperties = handlerProperties;
	}

	/* Setter and Getter methods for parms */
	public Boolean getIncludeOpTimestamp() {
		return includeOpTimestamp;
	}

	public void setIncludeOpTimestamp(Boolean includeOpTimestamp) {
		this.includeOpTimestamp = includeOpTimestamp;
	}

}
