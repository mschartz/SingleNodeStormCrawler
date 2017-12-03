package edu.upenn.cis.cis455.model;

import java.io.Serializable;
import java.util.ArrayList;

public class UserChannelEntry implements Serializable {

//	String user;
	ArrayList<String> channel;
	
	public UserChannelEntry()
	{
//		this.user = user;
		//this.channel = channel;
		
	}
	
	public void addChannel(String channel)
	{
		if(this.channel == null)
		{
			this.channel = new ArrayList<String>();
			
		}
		this.channel.add(channel);
	}
	public ArrayList<String>getChannel()
	{
		return this.channel;
	}
}
