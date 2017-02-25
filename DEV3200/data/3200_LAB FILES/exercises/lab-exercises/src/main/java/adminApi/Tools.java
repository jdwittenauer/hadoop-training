package adminApi;

import java.util.NavigableMap;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class Tools {

	public static String resultToString(byte[] row, byte[] family,
			byte[] qualifier, byte[] value) {
		StringBuilder strBuilder = new StringBuilder();
		strBuilder.append("Result with rowKey " + Bytes.toString(row) + " : ");
		strBuilder.append(" Family - " + Bytes.toString(family));
		strBuilder.append(" : Qualifier - " + Bytes.toString(qualifier));
		strBuilder.append(" : Value: " + Bytes.toLong(value));
		return strBuilder.toString();
	}

	public static String resultToString(Result result) {
		StringBuilder strBuilder = new StringBuilder();
		strBuilder.append("Result with rowKey "
				+ Bytes.toString(result.getRow()) + " : ");

		for (Cell kv : result.rawCells()) {
			strBuilder.append(" Family - "
					+ Bytes.toString(kv.getFamilyArray()));
			strBuilder.append(" : Qualifier - "
					+ Bytes.toString(kv.getQualifierArray()));
			strBuilder.append(" : Value: " + Bytes.toLong(kv.getValueArray())
					+ " ");
		}
		return strBuilder.toString();
	}

	public static String resultMapToString(Result result) {
		StringBuilder strBuilder = new StringBuilder();
		strBuilder.append("Result with rowKey "
				+ Bytes.toString(result.getRow()) + " : ");

		NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> familyMap = result
				.getMap();

		for (byte[] family : familyMap.keySet()) {
			strBuilder.append(" Family - " + Bytes.toString(family));

			NavigableMap<byte[], NavigableMap<Long, byte[]>> qualMap = familyMap
					.get(family);
			for (byte[] qual : qualMap.keySet()) {
				strBuilder.append(" : Qualifier - " + Bytes.toString(qual));

				NavigableMap<Long, byte[]> valueTsMap = qualMap.get(qual);
				for (Long tstamp : valueTsMap.keySet()) {

					byte[] valueBytes = valueTsMap.get(tstamp);

					strBuilder.append(" : Value: " + Bytes.toLong(valueBytes)
							+ " ");

				}

			}
		}

		return strBuilder.toString();
	}
}
