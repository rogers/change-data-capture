package com.rogers.cdc.api.mutations;


import java.io.IOException;

import com.rogers.cdc.api.schema.Table;

/**
 *
 * @author eugene.miretsky
 *
 * This is the interface one would implement for specific CDC systems (GoldenGate, etc. ) 
 */

//TODO: I think that a better solution would be to just provide an an Op/Column and Table interface. toMutation and toTable can be generic 
public abstract class  MutationMapper<OpT, TableT>{
	public abstract Mutation toMutation(OpT op)  throws IOException;
	protected abstract Table toTable(TableT table)  throws IOException;
}
