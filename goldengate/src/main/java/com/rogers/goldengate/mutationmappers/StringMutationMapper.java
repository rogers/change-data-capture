package com.rogers.goldengate.mutationmappers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.goldengate.atg.datasource.meta.ColumnMetaData;
import com.goldengate.atg.datasource.meta.TableMetaData;
import com.goldengate.atg.datasource.DsColumn;
import com.rogers.cdc.api.schema.Table;

// Maps Golden Gate operations to Mutations - Just get the string value of the mutation for now

public class StringMutationMapper extends AbstractMutationMapper{
	final private static Logger logger = LoggerFactory.getLogger(StringMutationMapper.class);
   
    @Override
    protected Object convertColumn(DsColumn column, int colType)  throws IOException{  	  	 
     // return column.getAfterValue(); 
      return column.getValue(); 
    }
    @Override
    protected Table toTable(TableMetaData tb){
    	 String tableName = tb.getTableName().getOriginalShortName().toLowerCase();
		 String databaseName = tb.getTableName().getOriginalSchemaName().toLowerCase();
		   
		 List<String> pkColumnNames = new ArrayList();
		 getPks( tb, pkColumnNames);
		 //We don't really care about the schema here, it should never be used 
		 Table table=  new Table(databaseName, tableName);
		 table.setSchema( null, pkColumnNames );
		 return table;
    }
    private void getPks( TableMetaData tb,  List<String> pkColumnNames){
		 
		   for(ColumnMetaData column : tb.getColumnMetaData()) {
               logger.debug("meta for column = {}", column.getColumnName()  );
               if ( column.isKeyCol()){
           	      pkColumnNames.add(column.getColumnName());	
		    	 }
		    } 
		   
	   }
    
   
}
