package social.model;

import org.apache.hadoop.hbase.util.Bytes;

import social.TimeUtil;

public class UserPostLT {

    public String id;
    public String userId;
    public String postId;
    public long ts;
    public static final String SEPARATOR = "0";

    public UserPostLT() {
    }

    public UserPostLT(String userId, String postId, long ts) {
        this.userId = userId;
        this.postId = postId;
        this.id = makeId(userId, postId);
        this.ts = ts;
    }

    public UserPostLT(String userId, String postId) {
        this.userId = userId;
        this.postId = postId;
        this.id = makeId(userId, postId);
        this.ts = TimeUtil.getEpoch();
    }

    //id = userid | postid key 
    // mapping between the user and the posted URL.
    public static String makeId(String userId, String postId) {
        String id = userId + SEPARATOR + postId;
        return id;
    }

    public static String getId(byte[] rowKey) {
        String key = Bytes.toString(rowKey);
        return key;
    }

    public static UserPostLT getIds(byte[] rowKey) {
        String key = getId(rowKey);
        return getIds(key);
    }

    public static String getPostId(byte[] rowKey) {
        String key = getId(rowKey);
        String postId = key.substring(key.indexOf(SEPARATOR) + 1, key.length());
        return postId;
    }

    public static UserPostLT getIds(String key) {
        String userId = key.substring(0, key.indexOf(SEPARATOR));
        String postId = key.substring(key.indexOf(SEPARATOR) + 1, key.length());
        UserPostLT uplt = new UserPostLT();
        uplt.id = key;
        uplt.userId = userId;
        uplt.postId = postId;
        return uplt;
    }

    public static UserPostLT createUserPostLT(String key, long ts) {
        UserPostLT post = getIds(key);
        post.ts = ts;
        return post;
    }

    public static UserPostLT createUserPostLT(byte[] rowKey, long ts) {
        UserPostLT post = getIds(rowKey);
        post.ts = ts;
        return post;
    }

    @Override
    public String toString() {
        return "\nUserPostLT{" + "id=" + id + ", userId=" + userId + ",\n postId=" + postId + ", ts=" + ts + '}';
    }
}
