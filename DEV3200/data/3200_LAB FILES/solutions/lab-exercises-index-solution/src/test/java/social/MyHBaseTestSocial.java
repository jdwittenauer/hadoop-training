package social;

import static org.junit.Assert.assertEquals;

import java.util.Date;
import java.util.List;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.junit.Before;
import org.junit.Test;

import social.dao.PostDAO;
import social.dao.UserDAO;
import social.dao.UserPostLTDAO;
import social.model.Category;
import social.model.Comment;
import social.model.Post;
import social.model.User;
import social.model.UserPostLT;

public class MyHBaseTestSocial {

    //private final static DateFormat dateArgFormat = new SimpleDateFormat("yyyyMMdd");

    @Before
    public void setup() throws Exception {
    }
    //PostDAO.TABLE_NAME

    @Test
    public void testCreateUserPost() throws Exception {
        HTableInterface utable = MockHTable.create();
        UserDAO userDao = new UserDAO(utable);
        userDao.addUser("cmcdonald", "carol", "mcdonald");
        User u = userDao.getUser("cmcdonald");
        System.out.println("------- Successfully added user " + u);
        assertEquals("mcdonald", u.lastName);
        HTableInterface ptable = MockHTable.create();
        PostDAO postDao = new PostDAO(ptable);
        Post p = new Post(Category.SCIENCE.getCode(), new Date(),
                "http://www.pbs.org/wgbh/pages/frontline/health-science-technology/hunting-the-nightmare-bacteria/dr-charles-knirsch-these-are-not-ruthless-decisions/",
                u.userId);
        postDao.addPost(p);
        List<Post> posts = postDao.getPostByCategory(Category.SCIENCE.getCode());
        Post p2 = posts.get(0);
        System.out.println("-----Successfully added post " + p2);
        assertEquals(p.postId, p2.postId);

    }

    @Test
    public void testCreateUserPostUserPostLT() throws Exception {
        HTableInterface utable = MockHTable.create();
        UserDAO userDao = new UserDAO(utable);
        userDao.addUser("cmcdonald", "carol", "mcdonald");
        User u = userDao.getUser("cmcdonald");
        System.out.println("------- Successfully added user " + u);
        assertEquals("mcdonald", u.lastName);
        HTableInterface ptable = MockHTable.create();
        PostDAO postDao = new PostDAO(ptable);
        Post p = new Post(Category.SCIENCE.getCode(), new Date(),
                "http://www.pbs.org/wgbh/pages/frontline/health-science-technology/hunting-the-nightmare-bacteria/dr-charles-knirsch-these-are-not-ruthless-decisions/",
                u.userId);
        postDao.addPost(p);
        Post p2 = postDao.getPost(p.postId);
        System.out.println("-----Successfully added post " + p2);
        assertEquals(p.postId, p2.postId);
        HTableInterface uptable = MockHTable.create();
        UserPostLTDAO upDao = new UserPostLTDAO(uptable);
        UserPostLT up = new UserPostLT(u.userId, p.postId);
        upDao.addUserPostLT(up);
        UserPostLT upl = upDao.getUserPostLT(up.id);
        System.out.println("==== Successfully added userPostLT " + upl);
        assertEquals(upl.id, up.id);
        List<String> ids = upDao.getPostIdsByUserId(u.userId);
        assertEquals(ids.get(0), p2.postId);

    }

    @Test
    public void testCreateUserPostUserPostLTComment() throws Exception {
        HTableInterface utable = MockHTable.create();
        UserDAO userDao = new UserDAO(utable);
        userDao.addUser("cmcdonald", "carol", "mcdonald");
        User u = userDao.getUser("cmcdonald");
        System.out.println("------- Successfully added user " + u);
        HTableInterface ptable = MockHTable.create();
        PostDAO postDao = new PostDAO(ptable);
        Post p = new Post(Category.SCIENCE.getCode(), new Date(),
                "http://www.pbs.org/wgbh/pages/frontline/health-science-technology/hunting-the-nightmare-bacteria/dr-charles-knirsch-these-are-not-ruthless-decisions/",
                u.userId);
        postDao.addPost(p);
        Post p2 = postDao.getPost(p.postId);
        System.out.println("-----Successfully added post " + p2);
        HTableInterface uptable = MockHTable.create();
        UserPostLTDAO upDao = new UserPostLTDAO(uptable);
        UserPostLT up = new UserPostLT(u.userId, p.postId);
        upDao.addUserPostLT(up);
        UserPostLT upl = upDao.getUserPostLT(up.id);
        System.out.println("==== Successfully added userPostLT " + upl);

        Comment c = new Comment(u.userId, "this pbs bacteria article is interesting");
        postDao.addComment(p.postId, c);
        System.out.println("----Successfully added comment " + c);

        List<Post> posts = postDao.getPostByCategory(Category.SCIENCE.getCode());
        Post p3 = posts.get(0);
        List<Comment> comments = postDao.getComments(p3.postId);
        assertEquals(comments.get(0).id, c.id);
        PostUtil.printPostsForAllCategories(postDao);
    
    }
}
