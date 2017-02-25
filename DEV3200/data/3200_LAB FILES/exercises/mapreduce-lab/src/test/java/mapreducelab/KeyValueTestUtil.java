package mapreducelab;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;

public class KeyValueTestUtil {
	public static KeyValue create(String row, String family, String qualifier,
			long timestamp, String value) {
		return new KeyValue(Bytes.toBytes(row), Bytes.toBytes(family),
				Bytes.toBytes(qualifier), timestamp,  Bytes.toBytes(value));
	}
	
	public static KeyValue create(String row, String family, String qualifier,
			long timestamp, long value) {
		return new KeyValue(Bytes.toBytes(row), Bytes.toBytes(family),
				Bytes.toBytes(qualifier), timestamp,  Bytes.toBytes(value));
	}

	public static KeyValue create(String row, String family, String qualifier,
			long timestamp, KeyValue.Type type, String value) {
		return new KeyValue(Bytes.toBytes(row), Bytes.toBytes(family),
				Bytes.toBytes(qualifier), timestamp, type, Bytes.toBytes(value));
	}

	protected static String getRowString(final KeyValue kv) {
		return Bytes.toStringBinary(kv.getRow());
	}

	protected static String getFamilyString(final KeyValue kv) {
		return Bytes.toStringBinary(kv.getFamily());
	}

	protected static String getQualifierString(final KeyValue kv) {
		return Bytes.toStringBinary(kv.getQualifier());
	}

	protected static String getTimestampString(final KeyValue kv) {
		return kv.getTimestamp() + "";
	}

	protected static String getTypeString(final KeyValue kv) {
		return KeyValue.Type.codeToType(kv.getType()).toString();
	}

	protected static String getValueString(final KeyValue kv) {
		return Bytes.toStringBinary(kv.getValue());
	}
}
