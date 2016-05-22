package com.rogers.cdc.api.schema;

import java.math.BigDecimal;
import java.net.URL;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Date;
import java.util.Calendar;


/**
 *    A interface for converting  the underlying column data to the proper Java type supported by Kafka Connect
 *    It's modeled after  com.mysql.jdbc.ResultSet, but return Strings/Byte[] instead of URL/Blob/Clob/etc. 
 *    If the underlying  data is a ResultSet object, this interface can be implemented  mostly a simple wrapper/proxy 
 *    None of the functions should return Null, throw excpetion if null....the user code is suppose to check for null and do something about it
 
 */
public interface AbstractSQLTypeConverter<T> {
	boolean getBoolean(T val) throws SQLException;
	byte[] getBytes(T val) throws SQLException;
	String getString(T val) throws SQLException;
	String getNString(T val) throws SQLException;
	Short getShort(T val) throws SQLException;
	Integer getInt(T val) throws SQLException;
	Long getLong(T val) throws SQLException;
	Float getFloat(T val) throws SQLException;
	Double getDouble(T val) throws SQLException;
	BigDecimal getBigDecimal(T val) throws SQLException; // in UTC
	Time getTime(T val,Calendar cal)throws SQLException; // in UTC
	Timestamp getTimestamp(T val,Calendar cal)throws SQLException;  // in UTC
	Date getDate(T val,Calendar cal)throws SQLException;
	String getClob(T val) throws SQLException;
	String getNClob(T val) throws SQLException;
	byte[] getBlob(T val) throws SQLException; 
	String getURL(T val) throws SQLException;
	String getSQLXML(T val) throws SQLException;
	boolean isNull(T val) throws SQLException;
}
