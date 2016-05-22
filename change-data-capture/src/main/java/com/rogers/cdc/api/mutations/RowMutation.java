package com.rogers.cdc.api.mutations;

//import com.rogers.cdc.api.mutations.Mutation;
//import com.rogers.cdc.api.mutations.Row;
//import com.rogers.goldengate.api.mutations.RowMutation;

import java.util.HashSet;
import java.util.Set;

import com.rogers.cdc.api.schema.*;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
//TODO: Do we really need the Row and Column classes?
//TODO: Should we do a sanity check that a mutation has a row? pKey?
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: All mutations seem to inherit from this. Just merge it into Mutation
public abstract class RowMutation extends Mutation {
	final private static Logger logger = LoggerFactory
			.getLogger(RowMutation.class);
	protected Row row;

	// protect Struct struct

	public RowMutation(Table table, Row _row) {
		super(table);
		row = _row;
	}

	public RowMutation(Table table) {
		this(table, null);

	}

	@Override
	public boolean equals(Object ob) {
		if (!super.equals(ob))
			return false;

		RowMutation other = (RowMutation) ob;
		if (!row.equals(other.row))
			return false;
		return true;
	}

	public Row getRow() {
		return row;
	}

	public Object getColumnVal(String name) {
		System.out.println(this);
		return row.getColumn(name);
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder();
		sb.append(this.getType()).append("{").append("\n");
		sb.append("  schema=").append(this.getSchemaName()).append("\n");
		sb.append("  table=").append(this.getTableName()).append("\n");
		sb.append("  row=[").append("\n");
		if (row != null) {
			sb.append(row.toString(5)).append("\n");
		}
		sb.append("  ]").append("\n");
		;
		sb.append("}");
		return sb.toString();
	}

	@Override
	public Struct getVal() {
		return this.getRow().toStruct(this.getTable().getSchema());
	}

	@Override
	public Struct getKey() {

		Schema schema = this.table.getKeySchema();
		Struct struct = new Struct(schema);
		logger.debug("toStruct: " + schema);
		for (String keyName : this.pKeyNames()) {

			Schema fieldSchema = schema.field(keyName).schema();
			Object val = this.row.getColumn(keyName);

			logger.debug("name = {}, col = {}, val = {},  schema = {}",
					keyName, val, fieldSchema);

			struct.put(keyName, Table.getSQLSchemaField(schema, keyName, val));
		}
		return struct;
	}

}
