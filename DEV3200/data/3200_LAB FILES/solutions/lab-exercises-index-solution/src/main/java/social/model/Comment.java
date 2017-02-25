package social.model;

import java.util.Date;
import org.apache.hadoop.hbase.util.Bytes;
import social.TimeUtil;

public class Comment {

    public static final String SEPARATOR = "|";
    public String id;
    public String userId;
    public Date date;
    public String text;

    public Comment() {
    }

    public Comment(String userId, String text) {
        this.userId = userId;
        this.date = new Date();
        this.id = makeId(userId, date);
        this.text = text;

    }

    public Comment(String userId, Date date, String text) {
        this.userId = userId;
        this.date = date;
        this.id = makeId(userId, date);
        this.text = text;
    }

    public Comment(Category c, String text, String userId) {
        this.userId = c.getCode();
        this.date = new Date();
        this.id = makeId(this.userId, date);
        this.text = text;
    }
    
    //id = userId  | TIMESTAMP 
    public static String makeId(String userId, Date date) {
        String timestamp = TimeUtil.dateToString(date);
        String id = userId + SEPARATOR + timestamp;
        return id;
    }

    public static String getId(byte[] rowKey) {
        String key = Bytes.toString(rowKey);
        return key;
    }

    public static Comment getIds(byte[] rowKey) {
        String key = getId(rowKey);
      //  System.out.println("comment  key " + key);
        return getIds(key);
    }

    public static Comment getIds(String key) {
        String userId = key.substring(0, key.indexOf(SEPARATOR));
        String sdate = key.substring(key.indexOf(SEPARATOR) + 1, key.length());
        Date date = TimeUtil.stringToDate(sdate);
        Comment comment = new Comment();
        comment.id = key;
        comment.userId = userId;
        comment.date = date;
        return comment;
    }

    public static Comment createComment(String key, String text) {
        Comment post = getIds(key);
        post.text = text;
        return post;
    }

    public static Comment createComment(byte[] rowKey, String text) {
        Comment post = getIds(rowKey);
        post.text = text;
        return post;
    }

    @Override
    public String toString() {
        return "\nComment{" + "\n id=" + id + ",\n userId=" + userId + ",\n date=" + date + ",\n text=" + text + "\n}";
    }
}
