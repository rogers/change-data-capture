package com.rogers.cdc.api.mutations;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rogers.cdc.api.schema.Table;
import com.rogers.cdc.kafka.KafkaUtil;

public abstract class Mutation implements Serializable {
	final private static Logger logger = LoggerFactory
			.getLogger(Mutation.class);
	//long tx_id; // TODO: figure out how we set it. Do we need it?
	protected byte magicByte;
	protected final Table table;
	
	

	public final static byte UnknownByte = 0x0;
	public final static byte InsertByte = 0x1;
	public final static byte UpdateByte = 0x2;
	public final static byte DeleteByte = 0x3;
	public final static byte UpdatePKByte = 0x4;

	Mutation(Table _table) {
		table = _table;
	}

	@Override
	public boolean equals(Object ob) {
		if (ob == null)
			return false;

		if (ob.getClass() != getClass()) return false;
		if (ob == this)
			return true;
		Mutation other = (Mutation) ob;
		if (!this.getTableName().equals(other.getTableName()))
			return false;
		if (!this.getTableName().equals(other.getTableName()))
			return false;
		return true;
	}

	public abstract MutationType getType();

	public String getTableName() {
		return table.getName();
	}

	// TODO: rename to DatabaseName
	public String getSchemaName() {
		return table.getDatabaseName();
	}

	// TODO: Some of the bellow methods should probably be private and moved
	// rowmutation
	public byte getMagicByte() {
		return magicByte;
	}

	abstract public Struct getKey();
	abstract public Struct getVal();

	public <T extends Mutation> T getMutation() {
		return (T) this;

	}

	public Table getTable() {
		return table;

	}

	// abstract public Set<Object> pKeysVals();
	protected List<String> pKeyNames() {
		return this.table.getPKList();
	}

	public void validate() {
		// TODO: Need to make sure it was initilzied properly.
		// Should be abstract?
		// Check rows for RowMutation too
	}

	public static Mutation fromStructKeyVal(String dbName, String tableName, Struct keyStruct,
			Struct valStruct) {
		Row row = Row.fromStruct(valStruct);
		Schema keySchema = (keyStruct == null) ? null : keyStruct.schema();
		Schema valSchema = (valStruct == null) ? null : valStruct.schema();
		Table table = new Table(dbName, tableName);
        List<String> pKeys = new ArrayList(); 
        for (org.apache.kafka.connect.data.Field field: keySchema.fields()){
        	pKeys.add(field.name());
        }
		table.setSchema(valSchema,pKeys);

		Mutation mutation;
		if (valStruct == null ) {
			mutation = new DeleteMutation(table, Row.fromStruct(keyStruct));
		} else if (row.size() == valSchema.fields().size()) {
			mutation = new InsertMutation(table, row);
		} else if(isValKey(row, pKeys)){
			mutation = new PkUpdateMutation(table,  Row.fromStruct(keyStruct), row);
		}else{
			mutation = new UpdateMutation(table, row);
		}
	
		return mutation;

	}
	
	private static boolean isValKey(Row row,  List<String> pKeys){
		if (row.size() != pKeys.size()){
			return false;
		}
        for (String key: pKeys){
        	if (row.getColumn(key) == null){
        		return false; 
        	}
        }
        return true;
		
		
	}

}
