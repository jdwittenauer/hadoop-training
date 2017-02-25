package social.model;

import java.util.Date;
import org.apache.hadoop.hbase.util.Bytes;
import social.TimeUtil;

public class Post {

    public static final String SEPARATOR = "|";
    public String postId;
    public String category;
    public Date date;
    public String url;
    public String userId = null;

    public Post() {
    }

    public Post(String category, String url, String userId) {
        this.category = category;
        this.date = new Date();
        this.postId = makeId(category, date);
        this.url = url;
        this.userId = userId;
    }

    public Post(String category, Date date, String url, String userId) {
        this.category = category;
        this.date = date;
        this.postId = makeId(category, date);
        this.url = url;
        this.userId = userId;
    }

    public Post(Category c, String url, String userId) {
        this.category = c.getCode();
        this.date = new Date();
        this.postId = makeId(category, date);
        this.url = url;
        this.userId = userId;

    }
    
    //id = category code  | REVERSEDTIMESTAMP 
    public static String makeId(String category, Date date) {
        String timestamp = TimeUtil.dateToReverseString(date);
        String postId = category + SEPARATOR + timestamp;
        return postId;
    }

    public static String getId(byte[] rowKey) {
        String key = Bytes.toString(rowKey);
    //    System.out.println("post  key " + key);
        return key;
    }

    public static Post getIds(byte[] rowKey) {
        String key = getId(rowKey);
        return getIds(key);
    }

    public static Post getIds(String key) {
        String category = key.substring(0, key.indexOf(SEPARATOR));
        String reverseDate = key.substring(key.indexOf(SEPARATOR) + 1, key.length());
        Date date = TimeUtil.reverseStringToDate(reverseDate);
        Post post = new Post();
        post.postId = key;
        post.category = category;
        post.date = date;
        return post;
    }

    public static Post createPost(String key, String url) {
        Post post = getIds(key);
        post.url = url;
        return post;
    }

    public static Post createPost(byte[] rowKey, String url, String userId) {
        Post post = getIds(rowKey);
        post.url = url;
        post.userId = userId;
        return post;
    }

    @Override
    public String toString() {
        return "\nPost{" + "\n key=" + postId + "\n category=" + category + ",\n date=" + date + ",\n url=" + url + ",\n userId=" + userId + "\n}";
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj); //To change body of generated methods, choose Tools | Templates.
    }
    
}
