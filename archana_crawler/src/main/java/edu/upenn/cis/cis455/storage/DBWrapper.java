package edu.upenn.cis.cis455.storage;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.bind.tuple.StringBinding;
import com.sleepycat.collections.StoredSortedMap;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.je.EnvironmentConfig;

import edu.upenn.cis.cis455.model.ChannelEntry;
import edu.upenn.cis.cis455.model.ContentSeenEntry;
import edu.upenn.cis.cis455.model.CorpusEntry;
import edu.upenn.cis.cis455.model.KeywordEntry;
import edu.upenn.cis.cis455.model.LexiconEntry;
import edu.upenn.cis.cis455.model.MatchedDocumentEntry;
import edu.upenn.cis.cis455.model.UrlEntry;
import edu.upenn.cis.cis455.model.User;
import edu.upenn.cis.cis455.model.UserChannelEntry;

public class DBWrapper implements StorageInterface {
	
	private String envDirectory = null;
	
	private Environment myEnv;
	private EntityStore store;
	
	private Map<String,User> userMap = new HashMap<>();
	private Map<Integer,CorpusEntry> corpus = new HashMap<>();
	private Map<Integer,UrlEntry> urls = new HashMap<>();
	private Map<String,Integer> invUrls = new HashMap<>();
	private Map<Integer,LexiconEntry> lex = new HashMap<>();
	private Map<String,Integer> invLex = new HashMap<>();
	private Map<Integer,List<KeywordEntry>> inverted = new HashMap<>();
	private StoredSortedMap<Integer,KeywordEntry> invertedIndex;
	private Map<String, ChannelEntry> channelMap = new HashMap<>();
	private Map<String, UserChannelEntry> userChannelMap = new HashMap<>();
	private Map<String, ContentSeenEntry> contentSeenMap = new HashMap<>();
	private Map<String, MatchedDocumentEntry> matchedDocMap = new HashMap<>();
	
	private static final String USER_STORE = "user_store";
    private static final String CORPUS_STORE = "corpus_store";
    private static final String URL_STORE = "url_store";
    private static final String INV_URL_STORE = "inv_url_store";
    private static final String LEX_STORE = "lex_store";
    private static final String INV_LEX_STORE = "inv_lex_store";
    private static final String OCCURRENCE_STORE = "occurrence_store";
    private static final String CHANNEL_STORE = "channel_store";
    private static final String USER_CHANNEL_STORE = "user_channel_store";
    private static final String CONTENT_SEEN_STORE = "content_seen_store";
    private static final String MATCHED_DOC_STORE = "matched_doc_store";
    
	private static final String CLASS_CATALOG = "java_class_catalog";
    private StoredClassCatalog javaCatalog;
    
//    private static final String saltKey = "PveFT7isDjGYFTaYhc2Fzw==";
    
    private Database userDb;
    private Database corpusDb;
    private Database urlDb;
    private Database invUrlDb;
    private Database lexDb;
    private Database invLexDb;
    private Database occurrenceDb;
    private Database channelDb;
    private Database userChannelDb;
    private Database contentSeenDb;
    private Database matchedDocDb;
	
	public DBWrapper(String envDirectory) {
	    this.envDirectory = envDirectory;
	    
	    EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setTransactional(true);
        envConfig.setAllowCreate(true);

        myEnv = new Environment(new File(envDirectory), envConfig);
        
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setTransactional(true);
        
        Database catalogDb = myEnv.openDatabase(null, CLASS_CATALOG, 
                                              dbConfig);

        javaCatalog = new StoredClassCatalog(catalogDb);
        
        userDb = myEnv.openDatabase(null, USER_STORE, dbConfig);
        corpusDb = myEnv.openDatabase(null, CORPUS_STORE, dbConfig);
        urlDb = myEnv.openDatabase(null, URL_STORE, dbConfig);
        invUrlDb = myEnv.openDatabase(null, INV_URL_STORE, dbConfig);
        lexDb = myEnv.openDatabase(null, LEX_STORE, dbConfig);
        invLexDb = myEnv.openDatabase(null, INV_LEX_STORE, dbConfig);
        
        occurrenceDb = myEnv.openDatabase(null, OCCURRENCE_STORE, dbConfig);     
        channelDb = myEnv.openDatabase(null, CHANNEL_STORE, dbConfig);
        userChannelDb = myEnv.openDatabase(null, USER_CHANNEL_STORE, dbConfig);
        contentSeenDb = myEnv.openDatabase(null, CONTENT_SEEN_STORE, dbConfig);
        matchedDocDb = myEnv.openDatabase(null, MATCHED_DOC_STORE, dbConfig);
        
        bindViews();
        
        
	}
	
	public void bindViews() {
	    EntryBinding<String> stringBinding = new StringBinding();
	    EntryBinding<Integer> intBinding = new IntegerBinding();
	    EntryBinding<ArrayList> arrayBinding = new SerialBinding<ArrayList>(javaCatalog, ArrayList.class);
	    EntryBinding<User> userBinding = new SerialBinding<User>(javaCatalog, User.class);
	    EntryBinding<CorpusEntry> corpusBinding = new SerialBinding<CorpusEntry>(javaCatalog, CorpusEntry.class);
	    EntryBinding<UrlEntry> urlBinding = new SerialBinding<UrlEntry>(javaCatalog, UrlEntry.class);
	    EntryBinding<LexiconEntry> lexiconBinding = new SerialBinding<LexiconEntry>(javaCatalog, LexiconEntry.class);
	    EntryBinding<KeywordEntry> keywordBinding = new SerialBinding<KeywordEntry>(javaCatalog, KeywordEntry.class);
	    EntryBinding<ChannelEntry> channelBinding = new SerialBinding<ChannelEntry>(javaCatalog, ChannelEntry.class);
	    EntryBinding<UserChannelEntry> userChannelBinding = new SerialBinding<UserChannelEntry>(javaCatalog, UserChannelEntry.class);
	    EntryBinding<ContentSeenEntry> contenSeenBinding = new SerialBinding<ContentSeenEntry>(javaCatalog, ContentSeenEntry.class);
	    EntryBinding<MatchedDocumentEntry> matchedDocBinding = new SerialBinding<MatchedDocumentEntry>(javaCatalog, MatchedDocumentEntry.class);
	    
	    userMap = new StoredSortedMap<String,User>(userDb, stringBinding, userBinding, true);
	    corpus = new StoredSortedMap<Integer,CorpusEntry>(corpusDb, intBinding, corpusBinding, true);
	    urls = new StoredSortedMap<Integer,UrlEntry>(urlDb, intBinding, urlBinding, true);
	    invUrls = new StoredSortedMap<String,Integer>(urlDb, stringBinding, intBinding, true);
	    lex = new StoredSortedMap<Integer,LexiconEntry>(urlDb, intBinding, lexiconBinding, true);
	    invLex = new StoredSortedMap<String,Integer>(urlDb, stringBinding, intBinding, true);
	    invertedIndex = new StoredSortedMap<Integer,KeywordEntry>(urlDb, intBinding, keywordBinding, true);
	    channelMap = new StoredSortedMap<String,ChannelEntry>(channelDb, stringBinding, channelBinding, true);
	    userChannelMap = new StoredSortedMap<String,UserChannelEntry>(channelDb, stringBinding, userChannelBinding, true);
	    contentSeenMap = new StoredSortedMap<String,ContentSeenEntry>(contentSeenDb, stringBinding, contenSeenBinding, true);
	    matchedDocMap = new StoredSortedMap<String,MatchedDocumentEntry>(contentSeenDb, stringBinding, matchedDocBinding, true);
	    
	}
	
	public void close()
        throws DatabaseException
    {
        userDb.close();
        corpusDb.close();
        urlDb.close();
        invUrlDb.close();
        lexDb.close();
        invLexDb.close();
        occurrenceDb.close();
        javaCatalog.close();
        myEnv.close();
        channelDb.close();
        userChannelDb.close();
        contentSeenDb.close();
        
    } 
	
	public final StoredClassCatalog getClassCatalog() {
        return javaCatalog;
    } 
	
	
	
	@Override
	public int getCorpusSize() {
	    return corpus.size();
	}
	
	@Override
	public int addDocument(String url, String document) {
		
		int docId;
		synchronized(corpus)
	    {
			docId = getCorpusSize() + 1;
	    
		    corpus.put(docId, new CorpusEntry(docId, document));
		    corpus.notifyAll();
	    }
		synchronized(urls)
	    {
		    urls.put(docId, new UrlEntry(docId, url));
		    urls.notifyAll();
	    }
		synchronized(invUrls)
	    {
		    invUrls.put(url, docId);
		    invUrls.notifyAll();
	    }
	    
	    
	    return docId;
	}
	
	@Override
	public String getDocument(String url) {

		Integer docId;
		synchronized(invUrls)
	    {
			docId = invUrls.get(url);
			invUrls.notifyAll();
	    }
		
	    if (docId == null)
	       return null;
	    else
	    {
	    	synchronized(corpus)
		    {
	    		String ret = corpus.get(docId).getContent();;
	    		corpus.notifyAll();
	    		return ret;
		    }
	    }
	       
	}

    @Override
    public int getLexiconSize() {
        return lex.size();
    }

    @Override
    public int addOrGetKeywordId(String keyword) {
        if (invLex.containsKey(keyword))
            return invLex.get(keyword);
        else {
            int count = lex.size();
            lex.put(count, new LexiconEntry(count, keyword));
            invLex.put(keyword, count);
            
            return count;
        }
    }
    
//    @Override
//    public void addHit(String keyword, Integer docId) {
//        
//    }

    @Override
    public int addUser(String username, String password) {
        
    	int next = 0;
    	synchronized(userMap)
        {
    		next = userMap.size();
	        //Check for uniqueness
	        if(userMap.containsKey(username))
	        {
	        	return 0;
	        }
//	        byte[] hash = null;
//	        try {
//		        MessageDigest digest = MessageDigest.getInstance("SHA-256");
//		        hash = digest.digest(password.getBytes(StandardCharsets.UTF_8));
//	        }
//	        catch(NoSuchAlgorithmException ie)
//	        {
//	        	
//	        }
//	        userMap.put(username, new User(next, username, hash.toString()));
	        userMap.put(username, new User(next, username, password));
	        userMap.notifyAll();
        }
        return next;
       
    }
    
    public void addNames(String username, String firstname, String lastname)
    {
    	synchronized(userMap)
        {
	    	int next = userMap.size();
	    	if(userMap.containsKey((username)))
	    	{
	    		User user1 = userMap.get("username");
	    		if(user1 != null)
	    		{
		    		user1.addNames(firstname, lastname);
		    		userMap.put(username, user1);
	    		}
	    	}
	    	userMap.notifyAll();
        }
    }

    @Override
    public boolean getSessionForUser(String username, String password) {
    	synchronized(userMap)
        {
	        if (userMap.get(username) != null)
	        {
//	        	byte[] hash = null;
//	        	byte[] salt = base64ToByte(saltKey);
//		        try {
//			        MessageDigest digest = MessageDigest.getInstance("SHA-256");
//			        
//			        hash = digest.digest(password.getBytes(StandardCharsets.UTF_8));
//		        }
//		        catch(NoSuchAlgorithmException ie)
//		        {
//		        	
//		        }
//		        String Password = userMap.get(username).getPassword();
//		        String hashPwd = hash.toString();
	        	if(userMap.get(username).getPassword().equals(password))
	        	{
		        	userMap.notifyAll();
		            return true;
	        	}
	        }
		       
        	userMap.notifyAll();
        	return false;
		       
        }
    }
    
    public String addChannel(String username, String channel, String xpath)
    {
    	//Add the channel to user
    	synchronized(userChannelMap)
        {
    	
	    	UserChannelEntry userChannel;
	    	if(!userChannelMap.containsKey(username))
	    	{
	    		userChannel = new UserChannelEntry();
	    		
	    	}
	    	else {
	    		userChannel = userChannelMap.get(username);
	    	}
	    	userChannel.addChannel(channel);
	    	userChannelMap.put(username, userChannel);
	    	userChannelMap.notifyAll();
        }
    	synchronized(channelMap)
        {	
	    	//Add the xpath to channel
	    	if(!channelMap.containsKey(channel))
	    	{
	    		System.out.println("Adding "+channel+" "+xpath);
	    		channelMap.put(channel, new ChannelEntry(channel,xpath));
	    	}
	    	else
	    	{
	    		channelMap.notifyAll();
	    		return null;
	    	}
	    	channelMap.notifyAll();
        }
    	
		return channel;
    }
	
    public Set<String> getChannelsFromXPaths()
    {
    	Set<String> keys;
    	synchronized(channelMap)
        {

    	keys = channelMap.keySet();
    	channelMap.notifyAll();
        }
    	return keys;
    }
    
    
    public String getXpath(String channel)
    {
    	synchronized(channelMap)
        {
	    	
    		
    		if(channelMap.containsKey(channel)) 
	    	{
	    		ChannelEntry channels = channelMap.get(channel);
	    		String ret = channels.getXpath();
	    		channelMap.notifyAll();
	    		return ret;
	    	}
    		channelMap.notifyAll();
        }
    		return null;
        
    	
    		
    }
    
    public ArrayList<String> getChannel(String username)
    {
    	synchronized(userChannelMap)
        {
	    	if(userChannelMap.containsKey(username)) {
	    		System.out.println("Returning channel ");
	    		userChannelMap.notifyAll();
	    		return userChannelMap.get(username).getChannel();
    		}
    	
    	userChannelMap.notifyAll();
        }
    	return null;
    }
    
    public void addContentSeen(String file)
    {
    	byte[] hash = null;
        try {
	        MessageDigest digest = MessageDigest.getInstance("MD5");
	        hash = digest.digest(file.getBytes(StandardCharsets.UTF_8));
        }
        catch(NoSuchAlgorithmException ie)
        {
        	
        }
        synchronized(contentSeenMap)
        {
        	contentSeenMap.put(hash.toString(), new ContentSeenEntry(hash.toString()));
        	contentSeenMap.notifyAll();
        }
    }
    
    public boolean isContentSeen(String file)
    {
    	byte[] hash = null;
        try {
	        MessageDigest digest = MessageDigest.getInstance("MD5");
	        hash = digest.digest(file.getBytes(StandardCharsets.UTF_8));
        }
        catch(NoSuchAlgorithmException ie)
        {
        	
        }
        synchronized(contentSeenMap)
        {
        	if(contentSeenMap.containsKey(hash.toString()))
        	{
        		contentSeenMap.notifyAll();
        		return true;
        	}
        	contentSeenMap.notifyAll();
        	return false;
        }
    }
    
    public void addMatchedDocs(String channel, String url, String doc)
    {
    	synchronized(matchedDocMap)
    	{
    		if(!matchedDocMap.containsKey(channel))
    		{
    			matchedDocMap.put(channel, new MatchedDocumentEntry());
    		}
    		matchedDocMap.get(channel).addMatchedDocs(url, doc);
    		matchedDocMap.notifyAll();
    	}
    }
    
    public Map<String, String> getMatchedDocs(String channel)
    {
    	synchronized(matchedDocMap)
    	{
    		if(!matchedDocMap.containsKey(channel))
    		{
    			matchedDocMap.notifyAll();
    			return null;
    		}
    		Map<String, String> ret = matchedDocMap.get(channel).getUrlDocMap();
    		matchedDocMap.notifyAll();
    		return ret;
    	}
    }
    
    public String getDocumentByDocId(int docId)
    {
    	synchronized(corpus)
    	{
    		String content = null;
    		if(corpus.containsKey(docId))
    		{
    			content = corpus.get(docId).getContent();
    		}
    		corpus.notifyAll();
    		return content;
    	}
    }
}
