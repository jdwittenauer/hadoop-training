package social;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import social.dao.UserDAO;
import social.model.User;

public class UserUtil {

    public static void printUsers(Configuration conf) throws IOException {
    
        UserDAO dao = new UserDAO(conf);
        List<User> users = dao.getUsers();
        System.out.println("\n======Print Users ================== ");
        System.out.println(String.format("Found %s users.", users.size()));
        for (User c : users) {
            System.out.println(c);
        }
        System.out.println("====================================== ");

    }

    public static User addUser(Configuration conf,String id, String firstName, String lastName) throws IOException {

        UserDAO dao = new UserDAO(conf);
        dao.addUser(id, firstName, lastName);
        User u = dao.getUser(id);
        System.out.println("------- Successfully added user " + u);

        return u;
    }

    public static void printUser(Configuration conf,String id) throws IOException {
 
        UserDAO dao = new UserDAO(conf);
        User u = dao.getUser(id);
        System.out.println("\n======Print User ================ ");
        System.out.println(u);
        System.out.println("=================================== ");
     
    }
}
