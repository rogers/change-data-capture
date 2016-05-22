package com.rogers.cdc.api.mutations;

//import com.rogers.goldengate.api.mutations.Mutation;
//import com.rogers.goldengate.api.mutations.MutationType;
import com.rogers.cdc.api.schema.*;

public class DeleteMutation extends RowMutation {
        //TODO: Maybe all mutations should have a key param? Or maybe just UpadtePk and Delete?   
	   /**
	    * A constructor for DeleteMutation
	    * Delete mutations don't have any column values, except the pKeys. row is used to hold the pKeys. 
	    * @param table
	    * @param _row
	    */
       public DeleteMutation(Table table, Row  _row){
       	super(table, _row);
       	magicByte = DeleteByte;
       }
	  @Override
	    public MutationType getType(){
	    	return MutationType.DELETE;
	    	
	    } 
	  
	    @Override
	    public String toString() {
	        
	        final StringBuilder sb = new StringBuilder();
	        sb.append(this.getType()).append("{").append("\n");
	        sb.append("  schema=").append(this.getSchemaName()).append("\n");
	        sb.append("  table=").append(this.getTableName()).append("\n");
	        sb.append("}");
	        return sb.toString();
	        
	    }
	     @Override
	      public boolean equals(Object ob) {
	    		if (!super.equals(ob)) return false;
	           if (ob.getClass() != getClass()) return false;

	        return true;
	      }
}