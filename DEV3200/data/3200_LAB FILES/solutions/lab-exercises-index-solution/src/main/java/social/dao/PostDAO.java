package social.dao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;

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

import social.model.Comment;
import social.model.Post;

public class PostDAO {

    public static final String userdirectory = ".";
  //  public static final String userdirectory = System.getProperty("user.home");
    public static final String tableName = userdirectory + "/post";
    public static final byte[] TABLE_NAME = Bytes.toBytes(tableName);
    public static final byte[] DATA_CF = Bytes.toBytes("data");
    public static final byte[] COMMENT_CF = Bytes.toBytes("com");
    // columns: url. userid
    public static final byte[] URL_COL = Bytes.toBytes("url");
    public static final byte[] UID_COL = Bytes.toBytes("uid");
    private static final Logger log = Logger.getLogger(PostDAO.class);
    private Configuration conf = null;
    private HTableInterface tableInterface = null;

	public PostDAO(Configuration conf) throws IOException{
		this.conf=conf;
	}

    public PostDAO(HTableInterface tableInterface) {
        this.tableInterface = tableInterface;
    }

    public static Get makeGet(String category, Date date) throws IOException {
        String id = Post.makeId(category, date);
        return makeGet(id);
    }

    public static Get makeGet(String id) throws IOException {
        return makeGet(Bytes.toBytes(id));
    }

    public static Get makeGetComment(String id) throws IOException {
        byte[] idb = Bytes.toBytes(id);
        Get g = new Get(idb);
        g.addFamily(COMMENT_CF);
        return g;
    }

    private static Get makeGet(byte[] id) throws IOException {
        Get g = new Get(id);
        g.addFamily(DATA_CF);
        return g;
    }

    private static Put makePut(Post post) {
        Put p = new Put(Bytes.toBytes(post.postId)); //
        p.add(DATA_CF, URL_COL, Bytes.toBytes(post.url));
        p.add(DATA_CF, UID_COL, Bytes.toBytes(post.userId));
        System.out.println("make Put   [" + post + "]");
        return p;
    }

    private static Put makePut(String postId, Comment comment) {
        Put p = new Put(Bytes.toBytes(postId)); //
        p.add(COMMENT_CF, Bytes.toBytes(comment.id), Bytes.toBytes(comment.text));
        System.out.println("make Put   [" + comment + "]");
        return p;
    }

    public static Put makePut(String id, byte[] fam, byte[] qual, byte[] val) {
        Put p = new Put(Bytes.toBytes(id));
        p.add(fam, qual, val);
        return p;
    }

    private static Delete makeDel(String id) {
        Delete d = new Delete(Bytes.toBytes(id));
        return d;
    }

    private static Scan makeScan() {
        Scan s = new Scan();
        s.addFamily(DATA_CF);
        return s;
    }

    public void addPost(String id, Date date, String url, String userId) throws IOException {
        addPost(new Post(id, date, url, userId));
    }

    public void addPost(String id, String url, String userId) throws IOException {
        addPost(new Post(id, url, userId));
    }

    public void addPost(Post post) throws IOException {
        HTableInterface table = getTable();
        Put p = makePut(post);
        table.put(p);
        table.close();
    }

    public void addComment(String postId, Comment comment) throws IOException {
        System.out.println(" add Comment " + postId + " " + comment);
        HTableInterface table = getTable();
        Put p = makePut(postId, comment);
        table.put(p);
        table.close();
    }

    public Post getPost(String category, Date date) throws IOException {
        String id = Post.makeId(category, date);
        return getPost(id);
    }

    public List<Comment> getComments(String postId) throws IOException {
        //    System.out.println(" getComments " + postId);
        HTableInterface table = getTable();
        Get g = makeGetComment(postId);
        Result result = table.get(g);
        if (result.isEmpty()) {
            log.info("comment not found.");
            return Collections.emptyList();
        }
        //getFamilyMap Returns a Map of the form: Map<qualifier,value>
        NavigableMap<byte[], byte[]> map = result.getFamilyMap(COMMENT_CF);
        List<Comment> comments = createComments(map);
        table.close();
        return comments;
    }

    public Post getPost(String postId) throws IOException {
        HTableInterface table = getTable();
        Get g = makeGet(postId);
        Result result = table.get(g);
        if (result.isEmpty()) {
            log.info("post  not found.");
            return null;
        }
        Post u = createPost(result);
        table.close();
        return u;
    }

    public void deletePost(String cart)
            throws IOException {
        HTableInterface table = getTable();
        Delete d = makeDel(cart);
        table.delete(d);
        table.close();

    }

    private static Scan makeScan(byte[] startRow, byte[] stopRow) {
        Scan s = new Scan(startRow, stopRow);
        s.addFamily(DATA_CF);
        return s;
    }

    private List<Post> getPosts(Scan scan) throws IOException {
        HTableInterface table = getTable();
        ResultScanner results = table.getScanner(scan);
        ArrayList<Post> ret = new ArrayList<Post>();
        for (Result r : results) {
            ret.add(createPost(r));
        }
        table.close();
        return ret;
    }

    public List<Post> getPosts() throws IOException {
        List<Post> ret = getPosts(makeScan());
        return ret;
    }

    public List<Post> getPostByCategory(String category) throws IOException {
        byte[] startRow = Bytes.toBytes(category);
        byte[] stopRow = Arrays.copyOf(startRow, startRow.length);
        stopRow[stopRow.length - 1]++;
        Scan scan = makeScan(startRow, stopRow);
        return getPosts(scan);
    }

    private List<Comment> createComments(NavigableMap<byte[], byte[]> map) {
        List<Comment> comments = new ArrayList<Comment>();
        Collection<byte[]> keys = map.keySet();
        Collection<byte[]> values = map.values();
        Iterator<byte[]> keyIterator = keys.iterator();
        Iterator<byte[]> valueIterator = values.iterator();
        while (keyIterator.hasNext() && valueIterator.hasNext()) {
            byte[] key = (byte[]) keyIterator.next();
            byte[] value = (byte[]) valueIterator.next();
            Comment c = createComment(key, value);
            //   System.out.println("got comment  " + c);
            comments.add(c);
        }
        return comments;
    }

    private Comment createComment(byte[] key, byte[] text) {
        return Comment.createComment(key, Bytes.toString(text));
    }

    private Post createPost(Result r) {
        return createPost(r.getRow(), r.getValue(DATA_CF, URL_COL), r.getValue(DATA_CF, UID_COL));
    }

    private Post createPost(byte[] key, byte[] url, byte[] uid) {
        return Post.createPost(key, Bytes.toString(url), Bytes.toString(uid));
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
