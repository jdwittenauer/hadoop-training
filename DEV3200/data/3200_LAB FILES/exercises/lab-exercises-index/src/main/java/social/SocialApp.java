package social;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import social.dao.PostDAO;
import social.dao.UserDAO;
import social.dao.UserPostLTDAO;
import social.model.Category;
import social.model.Post;
import social.model.User;
import social.model.UserPostLT;

public class SocialApp {

    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        if (args.length > 0 && args[0].equalsIgnoreCase("setup")) {
            System.out.println("-- Settingup user and Post Tables ...");
            //Creates the tables and sets some data for Post
            setupTables(conf);
            printTables(conf);
        }
        if (args.length > 0 && args[0].equalsIgnoreCase("print")) {
            System.out.println("---- Printing Tables ...");
            printTables(conf);
        }
        if (args.length == 0) {
            System.out.println("Usage for setup: java -cp `hbase classpath`:./labexercises-1.0.jar api.SocialApp setup ");
            System.out.println("Usage for checkout: java -cp `hbase classpath`:./labexercises-1.0.jar api.SocialApp checkout John or Mike or Mary or Adam. ");
        }
        if (args.length > 0 && args[0].equalsIgnoreCase("delete")) {
            System.out.println("--- Deleting user and Post Tables ...");
            deleteTables();
        }
    }

    public static void setupTables(Configuration conf) throws IOException {
      
        createTables(conf);
        //  Add data in  tables
        System.out.println("\n---------------- save data in tables ------------------");
        savePostUserData(conf);
        System.out.println("-------------------------------------------------------");
 
    }

    private static void deleteTables() throws IOException {
        System.out.println("\n---------------- delete tables ------------------");
        Configuration conf = HBaseConfiguration.create();
   
        TableUtil.deleteTable(conf, UserDAO.TABLE_NAME);
        TableUtil.deleteTable(conf, PostDAO.TABLE_NAME);
        TableUtil.deleteTable(conf, UserPostLTDAO.TABLE_NAME);
    }

    private static void createTables( Configuration conf) throws IOException {
        System.out.println("\n---------------- create tables ------------------");
        // Table 'Post' with column family 'data'
        TableUtil.createTable(conf, PostDAO.TABLE_NAME, PostDAO.DATA_CF, PostDAO.COMMENT_CF, 3);
        // Table 'user1' with column family 'data'
        TableUtil.createTable(conf, UserDAO.TABLE_NAME, UserDAO.DATA_CF, 3);
        // Table 'user1 post1 lookup' with column family 'data' max versions 1
        // TODO 3  create the  'user1 post1 lookup' table (UserPostLTDAO) with column family 'data' max versions 1
       // TableUtil.createTable(conf, UserPostLTDAO.TABLE_NAME, UserPostLTDAO.DATA_CF, 1);
    }

    private static void printTables(Configuration conf) throws IOException {
        System.out.println("----------------------------------------------------------");
        UserUtil.printUsers(conf);
        System.out.println("----------------------------------------------------------");
        PostUtil.printPosts(conf);
        System.out.println("----------------------------------------------------------");
        PostUtil.printPostsForAllCategories(conf);
        System.out.println("----------------------------------------------------------");
        
        // TODO 3  uncomment below
        UserPostLTUtil.printUserPostLTs(conf);
        System.out.println("----------------------------------------------------------");
        UserPostLTUtil.printUserPostIds(conf, "cmcdonald");
        System.out.println("----------------------------------------------------------");

    }

    /**
     *
     * @param conf
     * @throws IOException
     */
    public static void savePostUserData(Configuration conf) throws IOException {
        User user1 = UserUtil.addUser(conf,"cmcdonald", "carol", "mcdonald");
        System.out.println("-- added User " + user1);
        Post post1 = PostUtil.addPost(conf, Category.SCIENCE.getCode(), new Date(), "http://www.pbs.org/wgbh/pages/frontline/health-science-technology/hunting-the-nightmare-bacteria/dr-charles-knirsch-these-are-not-ruthless-decisions/",
                user1.userId);
        System.out.println("-- added Post " + post1);
        UserPostLT userPost = UserPostLTUtil.addUserPostLT(conf, user1.userId, post1.postId);
        System.out.println("-- added UserPostLT " + userPost);

        User user2 = UserUtil.addUser(conf,"jmbourgade", "John", "Bourgade");
        System.out.println("-- added User " + user2);
        Post post2 = PostUtil.addPost(conf, Category.NEWS.getCode(), new Date(), "http://www.cnn.com/2013/10/22/tech/mobile/new-ipads-hands-on/index.html?hpt=hp_bn5",
                user2.userId);
        System.out.println("-- added Post " + post2);
        userPost = UserPostLTUtil.addUserPostLT(conf,user2.userId, post2.postId);
        System.out.println("-- added UserPostLT " + userPost);

        User user3 = UserUtil.addUser(conf,"sreddy", "Sridhar", "Reddy");
        System.out.println("-- added User " + user3);
        Post post3 = PostUtil.addPost(conf, Category.BOOKS.getCode(), new Date(), "https://www.goodreads.com/book/show/18393609-david-and-goliath",
                user3.userId);
        System.out.println("-- added Post " + post3);
        
        User user4 = UserUtil.addUser(conf, "jsmith", "John", "Smith");
        System.out.println("-- added User " + user4);
        Post post4 = PostUtil.addPost(conf, Category.TECHNOLOGY.getCode(), new Date(), "http://www.cnn.com/2013/11/07/tech/mobile/apple-in-store-repair-iphone/index.html",
                user4.userId);
        System.out.println("-- added Post " + post4);
        Post post5 = PostUtil.addPost(conf, Category.ART.getCode(), new Date(), "http://www.huffingtonpost.com/2013/11/05/chagall-painting_n_4218316.html",
                user4.userId);
        System.out.println("-- added Post " + post5);
        Post post6 = PostUtil.addPost(conf, Category.SPORTS.getCode(), new Date(), "http://www.utsports.com/sports/m-footbl/",
                user4.userId);
        
    //  TODO 3  uncomment below
//        userPost = UserPostLTUtil.addUserPostLT(conf, user4.userId, post4.postId);
//        System.out.println("-- added UserPostLT " + userPost);
//        
//        userPost = UserPostLTUtil.addUserPostLT(conf, user4.userId, post5.postId);
//        System.out.println("-- added UserPostLT " + userPost);
//               
//        userPost = UserPostLTUtil.addUserPostLT(conf, user4.userId, post6.postId);
//        System.out.println("-- added UserPostLT " + userPost);
//        
//        userPost = UserPostLTUtil.addUserPostLT(conf, user3.userId, post3.postId);
//        System.out.println("-- added UserPostLT " + userPost);

        // TODO 4 uncomment below
//        PostUtil.addComment(conf, post1.postId, user3.userId, "this pbs bacteria article is interesting");
//        PostUtil.addComment(conf, post2.postId, user1.userId, "cnn I want an ipad");
//        PostUtil.addComment(conf, post3.postId, user2.userId, "good book");
//        PostUtil.addComment(conf, post1.postId, user2.userId, "pbs good science article");
//        PostUtil.addComment(conf, post1.postId, user4.userId, "interesting science article");
    }
}
