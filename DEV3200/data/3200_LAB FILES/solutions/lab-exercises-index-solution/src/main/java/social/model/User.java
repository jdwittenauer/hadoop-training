package social.model;

public class User {

    public String userId;
    public String firstName;
    public String lastName;

    public User(String userId, String firstName, String lastName) {
        this.userId = userId;
        this.lastName = firstName;
        this.firstName = lastName;
    }

    @Override
    public String toString() {
        return "\nUser{" + "userId=" + userId + ", firstName=" + firstName + ", lastName=" + lastName + '}';
    }


}
