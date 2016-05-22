package com.rogers.cdc.api.schema;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SQLDataConverter helps translating table schemas to Kafka Connect schemas
 * and row data to Kafka Connect records.
 */
public class SQLDataConverter {
	final private static Logger logger = LoggerFactory
			.getLogger(SQLDataConverter.class);

	private static final Calendar UTC_CALENDAR = new GregorianCalendar(
			TimeZone.getTimeZone("UTC"));


	// TODO: Wrap sqlType, fieldName and Optional in a "Type" struct
	static public void addFieldSchema(SchemaBuilder builder, int sqlType,
			String fieldName, boolean optional, int scale) {
		logger.debug("addFieldSchema: type = {}, name = {}, optional = {}",
				sqlType, fieldName, optional);
		switch (sqlType) {
		case Types.NULL: {
			logger.warn("JDBC type {} not currently supported", sqlType);
			break;
		}

		case Types.BOOLEAN: {

			if (optional) {
				builder.field(fieldName, Table.SQL_OPTIONAL_BOOLEAN_SCHEMA);
			} else {
				builder.field(fieldName, Table.SQL_BOOLEAN_SCHEMA);
			}
			break;
		}

		// ints <= 8 bits
		case Types.BIT:
		case Types.TINYINT: {
			if (optional) {
				builder.field(fieldName, Table.SQL_OPTIONAL_INT8_SCHEMA);
			} else {
				builder.field(fieldName, Table.SQL_INT8_SCHEMA);
			}
			break;
		}
		// 16 bit ints
		case Types.SMALLINT: {
			if (optional) {
				builder.field(fieldName, Table.SQL_OPTIONAL_INT16_SCHEMA);
			} else {
				builder.field(fieldName, Table.SQL_INT16_SCHEMA);
			}
			break;
		}

		// 32 bit ints
		case Types.INTEGER: {
			if (optional) {
				builder.field(fieldName, Table.SQL_OPTIONAL_INT32_SCHEMA);
			} else {
				builder.field(fieldName, Table.SQL_INT32_SCHEMA);
			}
			break;
		}

		// 64 bit ints
		case Types.BIGINT: {
			if (optional) {
				builder.field(fieldName, Table.SQL_OPTIONAL_INT64_SCHEMA);
			} else {
				builder.field(fieldName, Table.SQL_INT64_SCHEMA);
			}
			break;
		}

		// REAL is a single precision floating point value, i.e. a Java float
		case Types.REAL: {
			if (optional) {
				builder.field(fieldName, Table.SQL_OPTIONAL_FLOAT32_SCHEMA);
			} else {
				builder.field(fieldName, Table.SQL_FLOAT32_SCHEMA);
			}
			break;
		}

		// FLOAT is, confusingly, double precision and effectively the same as
		// DOUBLE. See REAL
		// for single precision
		case Types.FLOAT:
		case Types.DOUBLE: {
			if (optional) {
				builder.field(fieldName, Table.SQL_OPTIONAL_FLOAT64_SCHEMA);
			} else {
				builder.field(fieldName, Table.SQL_FLOAT64_SCHEMA);
			}
			break;
		}

		case Types.NUMERIC:
		case Types.DECIMAL: {

			SchemaBuilder subFieldBuilder = Decimal.builder(scale);
			if (optional) {
				subFieldBuilder.optional();
			}
			SchemaBuilder fieldBuilder = Table.sqlSchemaFor("dql_decimal",
					subFieldBuilder);
			builder.field(fieldName, fieldBuilder.build());
			break;
		}

		case Types.CHAR:
		case Types.VARCHAR:
		case Types.LONGVARCHAR:
		case Types.NCHAR:
		case Types.NVARCHAR:
		case Types.LONGNVARCHAR:
		case Types.CLOB:
		case Types.NCLOB:
		case Types.DATALINK:
		case Types.SQLXML: {
			// Some of these types will have fixed size, but we drop this from
			// the schema conversion
			// since only fixed byte arrays can have a fixed size
			if (optional) {
				builder.field(fieldName, Table.SQL_OPTIONAL_STRING_SCHEMA);
			} else {
				builder.field(fieldName, Table.SQL_STRING_SCHEMA);
			}
			break;
		}

		// Binary == fixed bytes
		// BLOB, VARBINARY, LONGVARBINARY == bytes
		case Types.BINARY:
		case Types.BLOB:
		case Types.VARBINARY:
		case Types.LONGVARBINARY: {
			if (optional) {
				builder.field(fieldName, Table.SQL_OPTIONAL_BYTES_SCHEMA);
			} else {
				builder.field(fieldName, Table.SQL_BYTES_SCHEMA);
			}
			break;
		}

		// Date is day + moth + year
		case Types.DATE: {

			SchemaBuilder subFieldBuilder = Date.builder();
			if (optional) {
				subFieldBuilder.optional();
			}
			SchemaBuilder dateSchemaBuilder = Table.sqlSchemaFor("sql_date",
					subFieldBuilder);
			builder.field(fieldName, dateSchemaBuilder.build());

			break;
		}

		// Time is a time of day -- hour, minute, seconds, nanoseconds
		case Types.TIME: {

			SchemaBuilder subFieldBuilder = Time.builder();
			if (optional) {
				subFieldBuilder.optional();
			}
			SchemaBuilder timeSchemaBuilder = Table.sqlSchemaFor("sql_time",
					subFieldBuilder);
			builder.field(fieldName, timeSchemaBuilder.build());
			break;
		}

		// Timestamp is a date + time
		case Types.TIMESTAMP: {

			SchemaBuilder subFieldBuilder = Timestamp.builder();
			if (optional) {
				subFieldBuilder.optional();
			}
			SchemaBuilder timeSchemaBuilder = Table.sqlSchemaFor(
					"sql_timestamp", subFieldBuilder);

			builder.field(fieldName, Table.SQL_TIMESTAMP_SCHEMA);
			break;
		}

		case Types.ARRAY:
		case Types.JAVA_OBJECT:
		case Types.OTHER:
		case Types.DISTINCT:
		case Types.STRUCT:
		case Types.REF:
		case Types.ROWID:
		default: {
			logger.warn("JDBC type {} not currently supported", sqlType);
			break;
		}
		}
		logger.debug("addFieldSchema {}: {}", fieldName,
				builder.field(fieldName));
	}

	public static <T> Object convertFieldValue(
			AbstractSQLTypeConverter<T> typeConvertor, T col, int colType)
			throws SQLException, IOException {
		// TODO: check if val is SQLNull
		// Should never get a null object anywhere here
		// If it's null - for updates, how do we know if it's a really null
		// (SQLNull), or missing field
		logger.debug("start convertFieldValue ");
		// boolean isSQLNull = false;
		// Check if SQL null
		if (typeConvertor.isNull(col)) {
			logger.debug("field is SQL null ");
			// isSQLNull = true;
			return null;
		}
		logger.debug(" convertFieldValue 2 ");
		final Object colValue;
		switch (colType) {
		case Types.NULL: {
			colValue = null;
			break;
		}

		case Types.BOOLEAN: {
			colValue = typeConvertor.getBoolean(col);
			break;
		}

		case Types.BIT: {
			/**
			 * BIT should be either 0 or 1. TODO: Postgres handles this
			 * differently, returning a string "t" or "f". See the
			 * elasticsearch-jdbc plugin for an example of how this is handled
			 */
			colValue = typeConvertor.getBytes(col);
			break;
		}

		// 8 bits int
		case Types.TINYINT: {
			colValue = typeConvertor.getBytes(col);
			break;
		}

		// 16 bits int
		case Types.SMALLINT: {
			colValue = typeConvertor.getShort(col);
			break;
		}

		// 32 bits int
		case Types.INTEGER: {
			colValue = typeConvertor.getInt(col);
			break;
		}

		// 64 bits int
		case Types.BIGINT: {
			colValue = typeConvertor.getLong(col);
			break;
		}

		// REAL is a single precision floating point value, i.e. a Java float
		case Types.REAL: {
			colValue = typeConvertor.getFloat(col);
			break;
		}

		// FLOAT is, confusingly, double precision and effectively the same as
		// DOUBLE. See REAL
		// for single precision
		case Types.FLOAT:
		case Types.DOUBLE: {
			colValue = typeConvertor.getDouble(col);
			break;
		}

		case Types.NUMERIC:
		case Types.DECIMAL: {
			colValue = typeConvertor.getBigDecimal(col);
			break;
		}

		case Types.CHAR:
		case Types.VARCHAR:
		case Types.LONGVARCHAR: {
			colValue = typeConvertor.getString(col);
			break;
		}

		case Types.NCHAR:
		case Types.NVARCHAR:
		case Types.LONGNVARCHAR: {
			colValue = typeConvertor.getNString(col);
			break;
		}

		// Binary == fixed, VARBINARY and LONGVARBINARY == bytes
		case Types.BINARY:
		case Types.VARBINARY:
		case Types.LONGVARBINARY: {
			colValue = typeConvertor.getBytes(col);
			break;
		}

		// Date is day + moth + year
		case Types.DATE: {
			colValue = typeConvertor.getDate(col, UTC_CALENDAR);
			break;
		}

		// Time is a time of day -- hour, minute, seconds, nanoseconds
		case Types.TIME: {
			colValue = typeConvertor.getTime(col, UTC_CALENDAR);
			break;
		}

		// Timestamp is a date + time
		case Types.TIMESTAMP: {
			logger.debug(" convertFieldValue:getTimestamp ");
			colValue = typeConvertor.getTimestamp(col, UTC_CALENDAR);
			break;
		}

		// Datalink is basically a URL -> string
		case Types.DATALINK: {
			colValue = typeConvertor.getURL(col);
			// colValue = (url != null ? url.toString() : null);
			break;
		}

		// BLOB == fixed
		case Types.BLOB: {
			/*
			 * Blob blob = typeConvertor.getBlob(col); if (blob == null) {
			 * colValue = null; } else { if (blob.length() > Integer.MAX_VALUE)
			 * { throw new
			 * IOException("Can't process BLOBs longer than Integer.MAX_VALUE");
			 * } colValue = blob.getBytes(1, (int) blob.length()); blob.free();
			 * }
			 */
			byte[] blob = typeConvertor.getBlob(col);
			if (blob.length > Integer.MAX_VALUE) {
				throw new IOException(
						"Can't process BLOBs longer than Integer.MAX_VALUE");
			}
			colValue = blob;
			break;
		}
		case Types.CLOB:
		case Types.NCLOB: {
			/*
			 * Clob clob = (colType == Types.CLOB ? typeConvertor.getClob(col) :
			 * typeConvertor.getNClob(col)); if (clob == null) { colValue =
			 * null; } else { if (clob.length() > Integer.MAX_VALUE) { throw new
			 * IOException("Can't process BLOBs longer than Integer.MAX_VALUE");
			 * } colValue = clob.getSubString(1, (int) clob.length());
			 * clob.free(); }
			 */
			String clob = (colType == Types.CLOB ? typeConvertor.getClob(col)
					: typeConvertor.getNClob(col));
			if (clob.length() > Integer.MAX_VALUE) {
				throw new IOException(
						"Can't process BLOBs longer than Integer.MAX_VALUE");
			}
			colValue = clob;
			break;
		}

		// XML -> string
		case Types.SQLXML: {
			colValue = typeConvertor.getSQLXML(col);

			break;
		}

		case Types.ARRAY:
		case Types.JAVA_OBJECT:
		case Types.OTHER:
		case Types.DISTINCT:
		case Types.STRUCT:
		case Types.REF:
		case Types.ROWID:
		default: {
			// These are not currently supported, but we don't want to log
			// something for every single
			// record we translate. There will already be errors logged for the
			// schema translation
			return null;
		}
		}
		logger.debug("convertFieldValue, return {}", colValue);
		return colValue;
		// FIXME: Would passing in some extra info about the schema so we can
		// get the Field by index
		// be faster than setting this by name?
		// struct.put(fieldName, typeConvertor.wasNull() ? null : colValue);
	}

}
