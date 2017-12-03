package edu.upenn.cis.cis455.model;

import java.io.Serializable;

public class ChannelEntry implements Serializable {

	String xpath;
	String channel;
	
	public ChannelEntry(String channel, String xpath)
	{
		this.xpath = new String(xpath);
		this.channel = new String(channel);
		
	}
	
	public String getXpath()
	{
		return this.xpath;
	}
}
