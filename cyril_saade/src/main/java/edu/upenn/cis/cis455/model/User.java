package edu.upenn.cis.cis455.model;

import java.io.Serializable;

public class User implements Serializable {
    Integer userId;
    String userName;
    String password;
    String firstname;
    String lastname;
    
    
    public User(Integer userId, String firstname, String lastname, String userName, String password) {
        this.userId = userId;
        this.userName = userName;
        this.password = password;
        this.firstname = firstname;
        this.lastname = lastname;
    }
    
    public Integer getUserId() {
        return userId;
    }
    
    public String getFirstName() {
        return firstname;
    }
    
    public String getLastName() {
        return lastname;
    }
    
    public String getUserName() {
        return userName;
    }
    
    public String getPassword() {
        return password;
    }
}
