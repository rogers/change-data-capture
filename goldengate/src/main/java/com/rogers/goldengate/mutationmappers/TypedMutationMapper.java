package com.rogers.goldengate.mutationmappers;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.goldengate.atg.datasource.meta.ColumnMetaData;
import com.goldengate.atg.datasource.meta.DsType;
import com.goldengate.atg.datasource.meta.TableMetaData;
import com.goldengate.atg.datasource.DsColumn;
import com.rogers.cdc.api.schema.AbstractSQLTypeConverter;
import com.rogers.cdc.api.schema.SQLDataConverter;
import com.rogers.cdc.api.schema.Table;


public class TypedMutationMapper extends AbstractMutationMapper {
	final private static Logger logger = LoggerFactory.getLogger(TypedMutationMapper.class);
	AbstractSQLTypeConverter<DsColumn> typeConvertor = new GGSQLTypeConverter();

	@Override
	protected Object convertColumn(DsColumn column, int colType) throws IOException{
		
		try {
		   return SQLDataConverter.convertFieldValue(typeConvertor, column, colType);
		} catch (IOException e) {
			//logger.warn("Ignoring record due to SQL error:", e);
	        throw new IOException("failed to convert " + column + "to SQL type " + colType);
	    } 
		catch (SQLException e) {
			//logger.warn("Ignoring record due to SQL error:", e);
			throw new IOException("failed to convert " + column + "to SQL type " + colType);
	    }
		catch (Exception e) {
			//logger.warn("Ignoring record due to SQL error:", e);
			throw new IOException("failed to convert " + column + "to SQL type " + colType);
	    }
	}
	 @Override
	   protected Table toTable(TableMetaData tb){
		   String tableName = tb.getTableName().getOriginalShortName().toLowerCase();
		   String databaseName = tb.getTableName().getOriginalSchemaName().toLowerCase();
		   Table table=  new Table(databaseName, tableName);
		   SchemaBuilder builder = SchemaBuilder.struct().name(table.schemaName());
		   
		   List<String> pkColumnNames = new ArrayList();
		   setSchema(builder, tb, pkColumnNames);
		   Schema schema = builder.build();
		  
		   table.setSchema( schema, pkColumnNames );
		   return table;
		  
	   }
	   // Returns a Schema and a list of primary keys 
	   private void setSchema( SchemaBuilder builder, TableMetaData tb, List<String> pkColumnNames){
		   for(ColumnMetaData column : tb.getColumnMetaData()) {
           //  logger.debug("meta for column = " + column.getColumnName()  );
               if ( column.isKeyCol()){
              	      pkColumnNames.add(column.getColumnName());
              	 
		    	 }
                 addFieldSchema(builder,column );
		    }    
	   }
	   // TODO: copied from the JDBC connector.... should be part of some abstract layer
	   private void addFieldSchema(SchemaBuilder builder, ColumnMetaData column){
		  DsType type =  column.getDataType();
		  int sqlType = type.getJDBCType();
		  String fieldName = column.getColumnName();
		  
		  boolean optional = column.isNullable(); 
		  // isNullable  always seem to be false.... and then the code breaks when the value is null. So assume it's always nullable 
		  optional = true; 
		  int scale = type.getScale();
		  SQLDataConverter.addFieldSchema(builder,sqlType,fieldName, optional, scale); 
		  
		  logger.debug("\t Col Name  = " + column.getColumnName());
		  logger.debug("\t toString = " + type.toString());
		  logger.debug("\t isNullable = " + column.isNullable());
		  logger.debug("\t getJDBCType = " + type.getJDBCType());
		  logger.debug("\t getPrecision = " + type.getPrecision());
		  logger.debug("\t getScale = " + type.getScale());
		  logger.debug("\t getGGDataType = " + type.getGGDataType());
		  logger.debug("\t getGGDataSubType = " + type.getGGDataSubType());

	   }

}
