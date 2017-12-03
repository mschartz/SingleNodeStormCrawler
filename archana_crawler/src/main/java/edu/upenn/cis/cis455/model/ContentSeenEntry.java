package edu.upenn.cis.cis455.model;

import java.io.Serializable;

public class ContentSeenEntry implements Serializable  {

	String docId;
	long last_modified_time;
	
	public ContentSeenEntry(String docId)
	{
		this.docId = docId;
	}
	public void addTime(String time)
	{
		
	}
	
	public long getLastModified()
	{
		return last_modified_time;
	}
}
