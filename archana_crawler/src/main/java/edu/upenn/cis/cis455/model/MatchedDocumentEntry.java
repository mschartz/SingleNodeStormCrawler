package edu.upenn.cis.cis455.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MatchedDocumentEntry {
	Map<String, String> urlDocMap;
	
	public MatchedDocumentEntry()
	{
		urlDocMap = new HashMap<>();
	}
	
	public void addMatchedDocs(String url, String doc)
	{
		urlDocMap.put(url, doc);
	}
	
//	public List<String> getMatchedDocs()
//	{
//		return docs;
//	}
	
	public Map<String,String> getUrlDocMap()
	{
		return urlDocMap;
	}

}
