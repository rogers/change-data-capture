package com.rogers.cdc.api.mutations;

import java.io.Serializable;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Field;


// TODO: Clean up the use of Struct... 
// Row could extend Struct...
/*
public class SchemaRow implements Serializable {
	// private Map<String, Column> columns;
	private Struct columns;

	public SchemaRow(Schema schema) {
		columns = new Struct(schema);
	}

	public SchemaRow(Struct struct) {
		columns = struct;
	}




	public void addColumn(String name, Object col) {
		// System.out.print(name);
		columns.put(name, col);
	}

	public Struct getColumns() {
		return columns;
	}


	Object getColumn(String name) {
		return columns.get(name);
	}



	@Override
	public boolean equals(Object ob) {
		if (ob == null)
			return false;
		if (ob.getClass() != getClass())
			return false;
		if (ob == this)
			return true;

		RegularRow other = (RegularRow) ob;
		Struct m1 = this.columns;
		Struct m2 = other.columns;

		return m1 == m2;
	}

	public Set<Map.Entry<String, Object>> entrySet() {
		Set<Map.Entry<String, Object>> set = new HashSet();
		for (Field field : columns.schema().fields()) {
			set.add(new AbstractMap.SimpleEntry<String, Object>(field.name(),
					columns.get(field)));
		}
		return set;
	}

}
*/
