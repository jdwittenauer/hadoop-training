package social;

import static social.PostUtil.printPost;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;


import social.dao.UserPostLTDAO;
import social.model.UserPostLT;

public class UserPostLTUtil {

    public static void printUserPostLTs(Configuration conf) throws IOException {
 
        UserPostLTDAO dao = new UserPostLTDAO(conf);
        List<UserPostLT> userPostLTs = dao.getUserPostLTs();
        System.out.println("\n====== Print User Post lookup table entries ===");
        System.out.println(String.format("Found %s userPostLTs.", userPostLTs.size()));
        for (UserPostLT c : userPostLTs) {
            System.out.println(c);
        }
        System.out.println("========================");
 
    }

    public static void printUserPostIds(Configuration conf,String userId) throws IOException {
     
        UserPostLTDAO dao = new UserPostLTDAO(conf);
        List<String> userPostIds = dao.getPostIdsByUserId(userId);
        System.out.println("\n======Print UserPost Ids for user ===" + userId);
        System.out.println(String.format("Found %s userPostIds.", userPostIds.size()));
        for (String s : userPostIds) {
            System.out.println(s);
        }
        System.out.println("========================");

    }

    public static UserPostLT addUserPostLT(Configuration conf,String userId, String postId) throws IOException {
        UserPostLT up = new UserPostLT(userId, postId);
        up = addUserPostLT(conf, up);
        return up;
    }

    public static UserPostLT addUserPostLT(Configuration conf,UserPostLT up) throws IOException {
   
        UserPostLTDAO dao = new UserPostLTDAO(conf);
        dao.addUserPostLT(up);
        UserPostLT c = dao.getUserPostLT(up.id);
        System.out.println("==== Successfully added userPostLT " + up);

        return c;
    }

    public static void printUserPostLT(Configuration conf,String userId, String postId) throws IOException {
        String id = UserPostLT.makeId(userId, postId);
        printPost(conf, id);
    }

    public static void printUserPostLT(Configuration conf,String id) throws IOException {
  
        UserPostLTDAO dao = new UserPostLTDAO(conf);
        System.out.println("======Print UserPost LT === " + id);
        UserPostLT u = dao.getUserPostLT(id);
        System.out.println(u);
    
    }
}
