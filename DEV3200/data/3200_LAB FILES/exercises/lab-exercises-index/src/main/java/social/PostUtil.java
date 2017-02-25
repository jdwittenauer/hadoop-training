package social;

import java.io.IOException;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import social.dao.PostDAO;
import social.model.Category;
import social.model.Comment;
import social.model.Post;

public class PostUtil {

    public static void printPosts(Configuration conf) throws IOException {
        PostDAO dao = new PostDAO(conf);
        List<Post> posts = dao.getPosts();
        System.out.println("\n------ Print  posts-------------------------");
        System.out.println("Found %s posts." + posts.size());
        for (Post p : posts) {
            System.out.println(p);
            // TODO 4 uncomment
           // printComment(dao, p);
        }
        System.out.println("-----------------------------------------------");
    }

    public static void printPostsForAllCategories(Configuration conf) throws IOException {
        Category[] categories = Category.values();
        for (Category category : categories) {
            printPostsForCategory(conf, category);
        }
    }

    public static void printPostsForAllCategories(PostDAO dao) throws IOException {
        Category[] categories = Category.values();
        for (Category category : categories) {
            printPostsForCategory(dao, category);
        }
    }

    public static void printPostsForCategory(Configuration conf,Category category) throws IOException {
  
        PostDAO dao = new PostDAO(conf);
        printPostsForCategory(dao, category);

    }

    public static void printPostsForCategory(PostDAO dao, Category category) throws IOException {
        List<Post> posts = dao.getPostByCategory(category.getCode());
        System.out.println("\n-------------- Print  post for category  " + category.name() + " -------------");
        System.out.println(String.format("Found %s posts.", posts.size()));
        for (Post p : posts) {
            System.out.println(p);
            // TODO 4 uncomment
           // printComment(dao, p);
        }
    }

    public static void addComment(Configuration conf,String postId, String userId, String text) throws IOException {
        Comment c = new Comment(userId, text);
        addComment(conf,postId, c);
    }

    public static void addComment(Configuration conf,String postId, Comment comm) throws IOException {
 
        PostDAO dao = new PostDAO(conf);
        dao.addComment(postId, comm);
        //  dao.getComments(postId);
        System.out.println("----Successfully added comment " + comm);

    }

    public static Post addPost(Configuration conf,Post post) throws IOException {
 
        PostDAO dao = new PostDAO(conf);
        dao.addPost(post);
        Post p = dao.getPost(post.postId);
        System.out.println("-----Successfully added post " + p);
    
        return p;
    }

    public static Post addPost(Configuration conf,String category, Date date, String url, String userId) throws IOException {
        Post p = new Post(category, date, url, userId);
        p = addPost(conf, p);
        return p;
    }

    public static void printPost(Configuration conf,String category, Date date) throws IOException {
        String id = Post.makeId(category, date);
        printPost(conf,id);
    }

    public static void printPost(Configuration conf,String id) throws IOException {

        PostDAO dao = new PostDAO(conf);
        Post p = dao.getPost(id);
        System.out.println("\n------ Print post--------------------------------");
        System.out.println(p);
        // TODO 4  uncomment
      //  printComment(dao, p);
        System.out.println("---------------------------------------------------");
 
    }
    //

    public static void printComments(Configuration conf,String id) throws IOException {
    
        PostDAO dao = new PostDAO(conf);
        List<Comment> comments = dao.getComments(id);
        for (Comment c : comments) {
            System.out.println(c);
        }
 
    }

    private static void printComment(PostDAO dao, Post p) throws IOException {
        List<Comment> comments = dao.getComments(p.postId);
        for (Comment c : comments) {
            System.out.println(c);
        }
    }
}
