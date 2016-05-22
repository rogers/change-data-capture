package com.rogers.cdc.kafka.consumers;


import com.rogers.cdc.api.mutations.*;


public class ConsoleConusmer extends KafkaMutationAvroConsumer {
	
	public ConsoleConusmer(final String topic, final String zkConnect, final String groupId){
		super(topic, zkConnect,groupId);
	}

	@Override
	protected void processInsertOp(InsertMutation op) {
		System.out.print(op);

	}

	@Override
	protected void processDeleteOp(DeleteMutation op) {
		System.out.print(op);

	}

	@Override
	protected void processUpdateOp(UpdateMutation op) {
		System.out.print(op);

	}

	@Override
	protected void processPkUpdateOp(Mutation op) {
		System.out.print(op);

	}

}
