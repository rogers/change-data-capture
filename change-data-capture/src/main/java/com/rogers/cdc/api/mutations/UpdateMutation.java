package com.rogers.cdc.api.mutations;

//import com.rogers.goldengate.api.mutations.MutationType;
//import com.rogers.goldengate.api.mutations.Row;
//import com.rogers.cdc.api.mutations.RowMutation;


import org.apache.kafka.connect.data.Struct;

import com.rogers.cdc.api.schema.*;

public class UpdateMutation extends RowMutation {

    
    public UpdateMutation(Table table, Row  _row ){
    	super(table, _row);
        magicByte = UpdateByte; 
    }
    public UpdateMutation(Table table ){
    	this(table, null);
    	
    }
    @Override
    public MutationType getType(){
    	return MutationType.UPDATE;	
    }
     
   
    
}
