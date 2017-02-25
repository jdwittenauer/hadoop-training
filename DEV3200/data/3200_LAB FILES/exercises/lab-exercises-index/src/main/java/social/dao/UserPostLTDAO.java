package social.dao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import social.model.UserPostLT;

public class UserPostLTDAO {

    public static final String userdirectory = ".";
  //  public static final String userdirectory = System.getProperty("user.home");
    public static final String tableName = userdirectory + "/UserPostLT";
    public static final byte[] TABLE_NAME = Bytes.toBytes(tableName);
    public static final byte[] DATA_CF = Bytes.toBytes("data");
    // columns: ts
    public static final byte[] TS_COL = Bytes.toBytes("ts");
    public static final String SEPARATOR = "!";
    public static final byte[] SEPARATOR_BYTES = Bytes.toBytes(SEPARATOR);
    //
    private static final Logger log = Logger.getLogger(UserPostLTDAO.class);
    private Configuration conf = null;
    private HTableInterface tableInterface = null;
    public static final byte[] ONE = new byte[]{1};

	public UserPostLTDAO(Configuration conf) throws IOException{
		this.conf=conf;
	}

    public UserPostLTDAO(HTableInterface tableInterface) {
        this.tableInterface = tableInterface;
    }

    public static Get makeGet(String userId, String postId) throws IOException {
        return makeGet(UserPostLT.makeId(userId, postId));
    }

    public static Get makeGet(String id) throws IOException {
        return makeGet(Bytes.toBytes(id));
    }

	private static Get makeGet(byte[] id) throws IOException {
		Get g = null; // TODO 3 finish 
		g.addFamily(DATA_CF);
		return g;
	}

	private static Put makePut(UserPostLT userPost) {
		System.out.println("make Put   [" + userPost + "]");
		// TODO 3 create a Put object passing the userPost.id as Bytes
		// using Bytes.toBytes
		Put p = null; // TODO new Put
		p.add(DATA_CF, TS_COL, Bytes.toBytes(userPost.ts));
		return p;
	}

    public static Put makePut(String id, byte[] fam, byte[] qual, byte[] val) {
        Put p = new Put(Bytes.toBytes(id));
        p.add(fam, qual, val);
        return p;
    }

    private static Delete makeDel(String id) {
        System.out.println(String.format("make Delete for %s", id));
        Delete d = new Delete(Bytes.toBytes(id));
        return d;
    }

    private static Scan makeScan() {
        Scan s = new Scan();
        s.addFamily(DATA_CF);
        return s;
    }

	private static Scan makeScan(byte[] startRow, byte[] stopRow) {
		// TODO 3 finish creating the scan object
		Scan s = null;
		s.addFamily(DATA_CF);
		return s;
	}

    public void addUserPostLT(String userId, String postId) throws IOException {
        addUserPostLT(new UserPostLT(userId, postId));
    }

    public void addUserPostLT(UserPostLT post) throws IOException {
        HTableInterface table = getTable();
        Put p = makePut(post);
        table.put(p);
        table.close();
    }

    public UserPostLT getUserPostLT(String userId, String postId) throws IOException {
        String id = UserPostLT.makeId(userId, postId);
        return getUserPostLT(id);
    }

    public UserPostLT getUserPostLT(String id) throws IOException {
        HTableInterface table = getTable();
        Get g = makeGet(id);
        Result result = table.get(g);
        if (result.isEmpty()) {
            log.info("userPost  not found.");
            return null;
        }
        UserPostLT u = createUserPostLT(result);
        table.close();
        return u;
    }

    public void deleteUserPostLT(String cart) throws IOException {
        HTableInterface table = getTable();
        Delete d = makeDel(cart);
        table.delete(d);
        table.close();

    }

    public List<UserPostLT> getUserPostLTs() throws IOException {
        HTableInterface table = getTable();
        ResultScanner results = table.getScanner(makeScan());
        ArrayList<UserPostLT> ret = new ArrayList<UserPostLT>();
        for (Result r : results) {
            ret.add(createUserPostLT(r));
        }
        table.close();
        return ret;
    }

	public List<String> getPostIdsByUserId(String userId) throws IOException {
		HTableInterface table = getTable();
		// TODO 3 set the startRow to the userid in Bytes
		byte[] startRow = null;
		// TODO exericse 3 set the stopRow to a copy of the startRow with last
		// byte +1
		byte[] stopRow = null;

		// TODO 3 call makeScan passing the start and stop rows
		Scan scan = null; // TODO call makeScan
		scan.setMaxVersions(1);

		ResultScanner results = table.getScanner(scan);
		List<String> ids = new ArrayList<String>();
		for (Result r : results) {
			ids.add(UserPostLT.getPostId(r.getRow()));
		}
		table.close();
		return ids;
	}

    private UserPostLT createUserPostLT(Result r) {
        return createUserPostLT(r.getRow(), r.getValue(DATA_CF, TS_COL));
    }

    private UserPostLT createUserPostLT(byte[] key, byte[] ts) {
        return UserPostLT.createUserPostLT(key, Bytes.toLong(ts));
    }

    public HTableInterface getTable() throws IOException{
        HTableInterface table;
        if (conf != null) {
            table  = new HTable(conf, TABLE_NAME);
        } else {
            table = tableInterface;
        }
        return table;
    }
}
