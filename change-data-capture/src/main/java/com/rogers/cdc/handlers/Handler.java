package com.rogers.cdc.handlers;

import java.io.Closeable;

import com.rogers.cdc.api.mutations.MutationMapper;

/**
 * An abstract handler for CDC operations. A CDC framework (such as GoldenGate)
 * can use it to submit change events/mutations. A target adapter for Kafka (or
 * HDFS, FLume, etc) should extend this class to provide specific implementation
 * 
 * As of now, this works only for  'push' type CDC frameworks (such as GoldenGate)
 * that 'push' Ops (CDC changes) by calling processOp.
 * 
 * The caller is responsible for check-pointing (aka - keeping track of which changes have been 
 * successfully processed and committed to a persistent storage )
 * 
 * 'Pull' frameworks that would require the handler periodically 'pull' for new CDC changes (like Kafka JDBC Connect)
 * could be implemented by creating wrapper that would pull the source system for changes, and then 'push' then to this Handler
 * The class would have to implement it's own check-pointing
 */
abstract public class Handler<Op, Table, OpMapper extends MutationMapper<Op, Table>>
		implements Closeable {
	/**
	 * A MutationMapper implementation that maps source CDC system events to
	 * Mutations
	 */
	protected OpMapper opMapper;

	protected Handler(OpMapper _opMapper) {
		opMapper = _opMapper;
	}
	
	/**
	 * Processes the operations synchronously 
	 * The caller assumes that the Op has been persisted when the funtion returns, and will mark it as finished
	 */ 
	public abstract void processOp(Op op);
	
	//TODO: Allow processing operations asynchronously. Would require a called to provide a callback for checkpointing
	//      the Op when processing is done.
	// public abstract void processOpAsync(Op op);

	// TODO: Define possible exceptions
	void handleAlter(Op op) {
		assert false : "Producer::handleAlter() not implimented";
	}

}
