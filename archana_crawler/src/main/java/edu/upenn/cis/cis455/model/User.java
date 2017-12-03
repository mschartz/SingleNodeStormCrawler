package edu.upenn.cis.cis455.model;

import java.io.Serializable;
import java.util.ArrayList;

public class User implements Serializable {
    Integer userId;
    String userName;
    String password;
    String firstname;
    String lastname;
//   ArrayList<String> channel;
//   int channelIndex;
    
    private static final long serialVersionUID = -914637420962253881L;
    public User(Integer userId, String userName, String password) {
        this.userId = userId;
        this.userName = userName;
        this.password = password;
        this.firstname = "";
        this.lastname = "";
//        this.channel = new String[100];
//        this.channelIndex = 0;
    }
    
    public Integer getUserId() {
        return userId;
    }
    
    public String getUserName() {
        return userName;
    }
    
    public String getPassword() {
        return password;
    }
    
    public void addNames(String firstname, String lastname)
    {
    	this.firstname = firstname;
        this.lastname = lastname;
    }
    
//    public void addChannel(String channel)
//    {
//    	if(this.channel == null)
//    	{
//    		this.channel = new ArrayList<String>();
//    	}
//    	System.out.println("Added "+channel+"to "+this.userName);
//    	this.channel.add(channel);
//    }
//    
//    public ArrayList<String> getChannel()
//    {
//    	
//    	return this.channel;
//    }
}
