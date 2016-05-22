package com.rogers.goldengate.mutationmappers;


import java.io.IOException;

import org.apache.kafka.connect.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.goldengate.atg.datasource.DsColumn;
import com.goldengate.atg.datasource.adapt.Op;
import com.goldengate.atg.datasource.meta.ColumnMetaData;
import com.goldengate.atg.datasource.meta.TableMetaData;
import com.rogers.cdc.api.mutations.DeleteMutation;
import com.rogers.cdc.api.mutations.InsertMutation;
import com.rogers.cdc.api.mutations.Mutation;
import com.rogers.cdc.api.mutations.MutationMapper;
import com.rogers.cdc.api.mutations.PkUpdateMutation;
import com.rogers.cdc.api.mutations.Row;
import com.rogers.cdc.api.mutations.UpdateMutation;
import com.rogers.cdc.api.schema.Table;

public abstract class AbstractMutationMapper   extends MutationMapper<Op,TableMetaData > {
		final private static Logger logger = LoggerFactory.getLogger(AbstractMutationMapper.class);
		// TODO: Why do we really need onlyChanged? If UPDATECOMPRESSE is on, GG will give us only changed columns anyway. Otherwise we probably want the full row
	    private Row createRow(Op op, Schema schema, boolean onlyChanged ) throws IOException{
	  	    Row row = new Row();
	        TableMetaData tbl_meta = op.getTableMeta(); 
			  int i = 0;
			  for(DsColumn column : op) {
	               ColumnMetaData col_meta = tbl_meta.getColumnMetaData(i);; 
	                // TODO: DO we really need key columns here? Yes, for now!. Considering adding a key field or row (since keys can be aggregates) to Mutation
	                 if (!onlyChanged || column.isChanged() || col_meta.isKeyCol()){
	                	 String name = col_meta.getColumnName(); 
	                	 if (column.getAfter() == null){
	                		 logger.warn("column {}.{} of SQL type {} in null" , tbl_meta.getTableName(), name , column,   col_meta.getDataType().getJDBCType());
	                	 }else{ 
			    		    logger.debug("\t convertColumn {} = {} colType =  {}" , name , column,   col_meta.getDataType().getJDBCType());
			    		                   
			    		    row.addColumn(name,convertColumn(column.getAfter(),col_meta.getDataType().getJDBCType()));
	                	 }
			    	 }
	             i++;
			    } 
	           logger.info("row: {} ", row);
			   return row;
	    }
	    // For now this is used only for Delete and UpdatePk where before Key will always  be there
	    private Row createBeforeKeyRow(Op op, Schema schema) throws IOException{
	  	    Row row = new Row();
	        TableMetaData tbl_meta = op.getTableMeta(); 
			  int i = 0;
			  for(DsColumn column : op) {
	               ColumnMetaData col_meta = tbl_meta.getColumnMetaData(i);; 
	                 if ( col_meta.isKeyCol()){
	                	 String name = col_meta.getColumnName(); 
	                	 row.addColumn(name,convertColumn(column.getBefore(),col_meta.getDataType().getJDBCType()));
			    	 }
	             i++;
			    } 
	          logger.info("row: {}", row);
			   return row;
	    }
	    // TODO: Should to Proper type conversaion
	    protected abstract Object convertColumn(DsColumn col, int colType) throws IOException;
	    
	   @Override
	   public  Mutation  toMutation(Op op)  throws IOException {
	   	  Row row;
	   	  Table table = toTable(op.getTableMeta());
	   	  Schema schema = table.getSchema();
	   	  switch(op.getOpType()){
	           case DO_INSERT: 
	           	    row = createRow(op, schema, false);
	      	        return new InsertMutation(table,  row);
	            case  DO_DELETE: 
	            	 //row = createRow(op, schema, false);
	       	        return new DeleteMutation(table, createBeforeKeyRow(op, schema));
	            case DO_UPDATE: 
	            case DO_UPDATE_FIELDCOMP: 
	            case DO_UPDATE_AC: 
	           	   row = createRow(op, schema, true);
	       	       return new UpdateMutation(table, row);
	            case DO_UPDATE_FIELDCOMP_PK:
	            	row = createRow(op, schema, true);
	      	        return new PkUpdateMutation(table,  createBeforeKeyRow(op, schema), row);
	             default:
	      	        //logger.error("The operation type " + op.getOpType() + " on  operation: table=[" + op.getTableName() + "]" + ", op ts=" + op.getTimestamp() + "is not supported");
	      	        throw new IllegalArgumentException("KafkaAvroHandler::getMagicByte Unknown operation type");                                                                            
	         }
	     }
	   
	
}
