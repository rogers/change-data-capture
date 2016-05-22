package com.rogers.goldengate.mutationmappers;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
//import java.sql.Timestamp;
import java.util.Calendar;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.DateTimeParser;
/*
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.DateTimeParser;
import org.joda.time.format.ISODateTimeFormat;
*/
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import oracle.sql.*;

import com.rogers.cdc.api.schema.AbstractSQLTypeConverter;
import com.goldengate.atg.datasource.DsColumn;

public class GGSQLTypeConverter implements AbstractSQLTypeConverter<DsColumn> {
	final private static Logger logger = LoggerFactory.getLogger(GGSQLTypeConverter.class);
    // TODO: Default time zone?
	@Override
	public boolean getBoolean(DsColumn col) throws SQLException{
		// Oracle doesn't support Booleans, but pl/sql does.... not sure what happens, or if this will ever get called
		String val =  getStringInt(col);
		throw new SQLException("boolean type not supported: {}", val);
		/*
		String val =  getStringInt(col);
		if  (val == "t")
			return true;
		else if(val == "f"){
			return false;
		}
		else{
			throw new SQLException("boolean column must be either 't' or 'f', got invalid value:" + val);
		}
		*/
	}

	@Override
	public byte[] getBytes(DsColumn col) throws SQLException{
		// TODO what encoding? 
		// Do we care to trim?
		String val =  getStringInt(col);
		return val.getBytes() ;
	}

	@Override
	public String getString(DsColumn col) throws SQLException{
		// TODO: should we use Oracle.sql.CHAR here?
		String val =  getStringInt(col);
		return val;
	}

	@Override
	public String getNString(DsColumn col) throws SQLException {
		// TODO Auto-generated method stub
		String val =  getStringInt(col);
		throw new SQLException("NString column not supported ");
	}

	@Override
	public Short getShort(DsColumn col) throws SQLException {
		String val =  getStringInt(col);
		return new NUMBER(val).shortValue();
	}

	@Override
	public Integer getInt(DsColumn col) throws SQLException{
		String val =  getStringInt(col);
		return new NUMBER(val).intValue();
	}

	@Override
	public Long getLong(DsColumn col) throws SQLException{

		String val =  getStringInt(col);
		return new NUMBER(val).longValue();
	}

	@Override
	public Float getFloat(DsColumn col) throws SQLException {
		String val =  getStringInt(col);
		return new NUMBER(val).floatValue();
	}

	@Override
	public Double getDouble(DsColumn col) throws SQLException {
		String val =  getStringInt(col);
		return new NUMBER(val).doubleValue();
	}

	//@Override
	/*public BigDecimal getBigDecimal(DsColumn col, int scale) throws SQLException {
		// TODO Is there more logic to it? Corner cases?
		// Can we do this faster? 

		String val =  getStringInt(col);
		BigDecimal res = new  BigDecimal(val);
		res.setScale(scale);
		return res;
	}*/
	@Override
	public BigDecimal getBigDecimal(DsColumn col) throws SQLException {
		// TODO What about scale? Make sure that GG passes a proper string representation of a BigDecimal 

		String val =  getStringInt(col);
		return new NUMBER(val).bigDecimalValue();
	}

	@Override
	public Time getTime(DsColumn col, Calendar cal) throws SQLException {

		String val =  getStringInt(col);
		return new DATE(val).timeValue(cal);
	}

	@Override
	public Timestamp getTimestamp(DsColumn col, Calendar cal) throws SQLException{
		// How Oracle handles timestamps seems to be a mystery: http://stackoverflow.com/questions/14700962/default-jdbc-date-format-when-reading-date-as-a-string-from-resultset
		// What I have been able to figure out from GoldenGate doc...
		// 
		/*String val =  getStringInt(col);
		DateTimeFormatter sdf =  ISODateTimeFormat.dateTime();
		//DateTimeFormat.forPattern("YYYY-MM-DD:HH:MI.SS.FFFFFF"); // TimeStamp - for some reason the format is a bit differnt....
		DateTimeParser[] parsers = { 
		        DateTimeFormat.forPattern("YYYY-MM-DD:HH:MI.SS.FFFFFF" ).getParser(),// TimeStamp - for some reason the format is a bit differnt....
		        DateTimeFormat.forPattern( "yyyy-MM-dd" ).getParser() 
		        		};
		DateTimeFormatter formatter = new DateTimeFormatterBuilder().append( null, parsers ).toFormatter();

		return new Timestamp(sdf.parseDateTime(val).toDateTime(DateTimeZone.UTC).getMillis());*/
		
		
		/// NVM - just using oracle.sql types to parse everything
		
		// TODO: How do we know if there is a timezone? Maybe we can get it from getGGDataType/getGGDataSubType
		// Use TIMESTAMPTZ/TIMESTAMPLTZ  for time zones
		// GG specific behavior For TIMESTAMP WITH TIME ZONE, 3 options
		//   1) Default GG Extract aborts 
		//   2) INCLUDEREGIONID is set format is YYYY-MM-DD HH:MI.SS.FFFFFF in UTC
		//   3) INCLUDEREGIONIDWITHOFFSET is set  the format is YYYY-MM-DD HH:MI.SS.FFFFFF TZH:TZM (aka - time zone code is added) 
		String val =  getStringInt(col);
		// One would think that Oracle.sql.TIMESTAMP would be able to parse GG time stamps.... not that case :(
		// We try a bunch of ways to prase until one hopefully works.
		logger.debug("getTimestamp value {}", val);
		try {
			return new TIMESTAMP(val).timestampValue(cal);
		} catch (Exception e){
			logger.debug("getTimestamp try parsers");
			DateTimeParser[] parsers = { 
					DateTimeFormat.forPattern( "yyyy-MM-dd:HH:mm:ss.SSSSSS Z" ).getParser(),
			        DateTimeFormat.forPattern("yyyy-MM-dd:HH:mm:ss.SSSSSS" ).getParser(),// TimeStamp - for some reason the format is a bit different from the GG doc and JDBC standard
			        DateTimeFormat.forPattern("yyyy-MM-dd:HH:mm:ss" ).getParser()
			        		};
			DateTimeFormatter formatter = new DateTimeFormatterBuilder().append( null, parsers ).toFormatter();
			DateTime time = formatter.parseDateTime(val);
			logger.debug("time = {}", time);
			if (time != null){
			    return new Timestamp(time.toDateTime(DateTimeZone.forTimeZone(cal.getTimeZone())).getMillis());	
			}
			else{
				logger.warn("Timestamp value {} couldn't be parsed into a valid TimeStamp", val);
				return null;
			}
		}
		
	}

	@Override
	public Date getDate(DsColumn col, Calendar cal) throws SQLException{
		// TODO Oracle doc seems to say that DATE also stores time info...? http://docs.oracle.com/cd/B19306_01/server.102/b14225/ch4datetime.htm#i1006050

		String val =  getStringInt(col);
		return new DATE(val).dateValue(cal);
	}

	@Override
	public String getClob(DsColumn col) throws SQLException{
		// TODO Auto-generated method stub
		
		String val =  getStringInt(col);
		return val;
	}

	@Override
	public String getNClob(DsColumn col) throws SQLException{
		//String val =  getStringInt(col);
		throw new SQLException("NClob column not supported ");
	}

	@Override
	public byte[] getBlob(DsColumn col) throws SQLException{
		// TODO How do we convert a string to a Blob? Which encoding do we user?
		//Cannot user Oracle.sql.blob because it expects a connection
		throw new SQLException("BLOB column not supported ");
		//this.getBytes(col);
		//return null;
	}

	@Override
	public String getURL(DsColumn col) throws SQLException{
		// TODO Do some extra checks here?
		throw new SQLException("URL column not supported ");
	//	String val =  getStringInt(col);
		//return val;
	}

	@Override
	public String getSQLXML(DsColumn col) throws SQLException{
		// TODO Some extra checks
		throw new SQLException("SQLXML column not supported ");
		//String val =  getStringInt(col);
		//return val;
	}

	@Override
	public boolean isNull(DsColumn col) throws SQLException{
		//Return true if SQL null. 
       // if (col.getAfter() == null) 
		 if (col == null) 
        	throw new SQLException("GG Column cannot be Null (col.getAfter() probably returned null)");
		//return col.getAfter().isValueNull();
		 return col.isValueNull();
	}
	
	private String getStringInt (DsColumn col) throws SQLException{
		//logger.debug("getStringInt");
		String val = col.getAfterValue();
		if (val == null){
			throw new SQLException("column val should not be null");
		}
		return val;
	}
	

}
