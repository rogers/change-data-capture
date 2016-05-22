package com.rogers.cdc.api.mutations;

import com.rogers.cdc.api.schema.*;

public class PassThroughMutationMapper extends MutationMapper<Mutation, Table>{
	@Override
	public Mutation toMutation(Mutation op){
		return op;
	}
	protected Table toTable(Table t){
		return t;
	}
}
