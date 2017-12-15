package edu.upenn.cis.cis455.storage;

import com.sleepycat.je.DatabaseException;
import edu.upenn.cis.cis455.model.ChannelEntry;
import java.util.Map;
import java.util.List;

public interface StorageInterface {
    
    /**
     * How many documents so far?
     */
	public int getCorpusSize();
	
	/**
	 * Add a new document, getting its ID
	 */
	public int addDocument(String url, String documentContents);
	
	/**
	 * How many keywords so far?
	 */
	public int getLexiconSize();
	
	/**
	 * Gets the ID of a word (adding a new ID if this is a new word)
	 */
	public int addOrGetKeywordId(String keyword);
	
	/**
	 * Adds a user and returns an ID
	 */
	public int addUser(String firstname, String lastname, String username, String password);

	/*
	 * Adds url -> outgoingLinks for use in page rank
	 */

	public void addPageRankRecord(String url, String outLinks);
	public String getPageRankRecord(String url);

	/**
	 * Tries to log in the user, or else throws a HaltException
	 */
	public boolean getSessionForUser(String username, String password);
	public String getUserFullName(String username, String password);
	
	/**
	 * Retrieves a document's contents by URL
	 */
	public String getDocument(String url);
	
	/**
	 * Returns the timestamp of the document
	 */
	public Long getDocumentTimeStamp(String url);
	
	/**
	 * Returns true if the MD5 hash of the document was already stored
	 */
	public boolean hasSeen(String hash);
	
	public void insertSeen(String hash);
	public void close() throws DatabaseException;
	
	public void addSubscription(String username, String channelName);
	public ChannelEntry getSubscriptions(String username);
	public void addXPath(String channelName, String xpath);
	public String getXpath(String channelName);
	public Map<String, String> getAllXPaths();
	
	public void addMatch(String channelName, String documentID);
	public List<String> getMatches(String channelName);
	
}

