package edu.upenn.cis.cis455.storage;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

import edu.upenn.cis.cis455.model.CorpusEntry;
import edu.upenn.cis.cis455.model.KeywordEntry;
import edu.upenn.cis.cis455.model.LexiconEntry;
import edu.upenn.cis.cis455.model.UrlEntry;
import edu.upenn.cis.cis455.model.User;
import edu.upenn.cis.cis455.model.ChannelEntry;
import edu.upenn.cis.cis455.model.DocumentMatches;

public class DBWrapper implements StorageInterface {
	
	private String envDirectory = null;
	
	private Environment myEnv;
	private EntityStore store;
	
	private Map<String,User> userMap = new HashMap<>();
	private Map<String,String> pageRankMap = new HashMap<>();

	private Map<Integer,CorpusEntry> corpus = new HashMap<>();
	private Map<Integer,UrlEntry> urls = new HashMap<>();
	private Map<String,Integer> invUrls = new HashMap<>();
	private Map<Integer,LexiconEntry> lex = new HashMap<>();
	private Map<String,Integer> invLex = new HashMap<>();
	private Map<Integer,List<KeywordEntry>> inverted = new HashMap<>();
	private StoredSortedMap<Integer,KeywordEntry> invertedIndex;
	private Map<String, Integer> contentSeen = new HashMap<>();
	
	private Map<String, String> registeredXPaths = new HashMap<>(); // channelName->xpath
	private Map<String, ChannelEntry> userSubscriptions = new HashMap<>(); // username->subscribed channels
	private Map<String, DocumentMatches> channelToMatches = new HashMap<>();
	
	private static final String USER_STORE = "user_store";
	private static final String PAGE_RANK_STORE = "page_rank_store";

	private static final String CORPUS_STORE = "corpus_store";
    private static final String URL_STORE = "url_store";

	private static final String INV_URL_STORE = "inv_url_store";
    private static final String LEX_STORE = "lex_store";
    private static final String INV_LEX_STORE = "inv_lex_store";
    private static final String OCCURRENCE_STORE = "occurrence_store";
    private static final String CONTENT_SEEN_STORE = "content_seen_store";
    
    private static final String REGISTERED_XPATHS_STORE = "registered_xpaths_store";
	private static final String USER_SUBSCRIPTIONS_STORE = "user_subscriptions_store";
	private static final String CHANNEL_TO_MATCHES_STORE = "channel_to_matches_store";
	
	private static final String CLASS_CATALOG = "java_class_catalog";
    private StoredClassCatalog javaCatalog;
    
    private Database userDb;
	private Database pageRankDb;

	private Database corpusDb;
    private Database urlDb;
    private Database invUrlDb;
    private Database lexDb;
    private Database invLexDb;
    private Database occurrenceDb;
    private Database contentSeenDb;
    
    private Database registeredXPathsDb;
    private Database userSubscriptionsDb;
    private Database channelToMatchesDb;
	
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
		pageRankDb = myEnv.openDatabase(null, PAGE_RANK_STORE, dbConfig);

		corpusDb = myEnv.openDatabase(null, CORPUS_STORE, dbConfig);
        urlDb = myEnv.openDatabase(null, URL_STORE, dbConfig);
        invUrlDb = myEnv.openDatabase(null, INV_URL_STORE, dbConfig);
        lexDb = myEnv.openDatabase(null, LEX_STORE, dbConfig);
        invLexDb = myEnv.openDatabase(null, INV_LEX_STORE, dbConfig);
        
        occurrenceDb = myEnv.openDatabase(null, OCCURRENCE_STORE, dbConfig);
        
        contentSeenDb = myEnv.openDatabase(null, CONTENT_SEEN_STORE, dbConfig);
        
        registeredXPathsDb = myEnv.openDatabase(null, REGISTERED_XPATHS_STORE, dbConfig);
        userSubscriptionsDb = myEnv.openDatabase(null, USER_SUBSCRIPTIONS_STORE, dbConfig);
        channelToMatchesDb = myEnv.openDatabase(null, CHANNEL_TO_MATCHES_STORE, dbConfig);
        
        bindViews();
	}
	
	public void bindViews() {
	    EntryBinding<String> stringBinding = new StringBinding();
	    EntryBinding<Integer> intBinding = new IntegerBinding();
	    EntryBinding<User> userBinding = new SerialBinding<User>(javaCatalog, User.class);
	    EntryBinding<CorpusEntry> corpusBinding = new SerialBinding<CorpusEntry>(javaCatalog, CorpusEntry.class);
	    EntryBinding<UrlEntry> urlBinding = new SerialBinding<UrlEntry>(javaCatalog, UrlEntry.class);
	    EntryBinding<LexiconEntry> lexiconBinding = new SerialBinding<LexiconEntry>(javaCatalog, LexiconEntry.class);
	    EntryBinding<KeywordEntry> keywordBinding = new SerialBinding<KeywordEntry>(javaCatalog, KeywordEntry.class);
	    
	    EntryBinding<ChannelEntry> channelBinding = new SerialBinding<ChannelEntry>(javaCatalog, ChannelEntry.class);
	    EntryBinding<DocumentMatches> matchesBinding = new SerialBinding<DocumentMatches>(javaCatalog, DocumentMatches.class);
	    
	    userMap = new StoredSortedMap<String,User>(userDb, stringBinding, userBinding, true);
		pageRankMap = new StoredSortedMap<String,String>(pageRankDb, stringBinding, stringBinding, true);

		corpus = new StoredSortedMap<Integer,CorpusEntry>(corpusDb, intBinding, corpusBinding, true);
	    urls = new StoredSortedMap<Integer,UrlEntry>(urlDb, intBinding, urlBinding, true);
	    invUrls = new StoredSortedMap<String,Integer>(urlDb, stringBinding, intBinding, true);
	    lex = new StoredSortedMap<Integer,LexiconEntry>(urlDb, intBinding, lexiconBinding, true);
	    invLex = new StoredSortedMap<String,Integer>(urlDb, stringBinding, intBinding, true);
	    invertedIndex = new StoredSortedMap<Integer,KeywordEntry>(urlDb, intBinding, keywordBinding, true);
	    
	    contentSeen = new StoredSortedMap<String, Integer>(contentSeenDb, stringBinding, intBinding, true);
	    
	    registeredXPaths = new StoredSortedMap<String, String>(registeredXPathsDb, stringBinding, stringBinding, true);
	    userSubscriptions = new StoredSortedMap<String, ChannelEntry>(userSubscriptionsDb, stringBinding, channelBinding, true);
	    channelToMatches = new StoredSortedMap<String, DocumentMatches>(channelToMatchesDb, stringBinding, matchesBinding, true);
	}
	
	public void close()
        throws DatabaseException
    {
        channelToMatchesDb.close();
        registeredXPathsDb.close();
        userSubscriptionsDb.close();
		userDb.close();
		pageRankDb.close();
        contentSeenDb.close();
        corpusDb.close();
        urlDb.close();
        invUrlDb.close();
        lexDb.close();
        invLexDb.close();
        occurrenceDb.close();
        javaCatalog.close();
        myEnv.close();
        
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
	    int docId = getCorpusSize() + 1;
	    
	    corpus.put(docId, new CorpusEntry(docId, document));
	    urls.put(docId, new UrlEntry(docId, url));
	    invUrls.put(url, docId);
	    
	    return docId;
	}
	
	@Override
	public String getDocument(String url) {
	    Integer docId = invUrls.get(url);
	    
	    if (docId == null)
	       return null;
	    else
	       return corpus.get(docId).getContent();
	}
	
	@Override
	public void addSubscription(String username, String channelName) {
	    ChannelEntry subs = userSubscriptions.get(username);
	    if(subs == null)
	       subs = new ChannelEntry();
	    
	    subs.addChannel(channelName);
	    userSubscriptions.put(username, subs);
	}
	
	@Override
	public ChannelEntry getSubscriptions(String username) {
	    return userSubscriptions.get(username);
	}
	
	@Override
	public void addXPath(String channelName, String xpath) {
	    registeredXPaths.put(channelName, xpath);
	}
	
	@Override
	public String getXpath(String channelName) {
	    return registeredXPaths.get(channelName);
	}
	
	@Override
	public Map<String, String> getAllXPaths() {
	    return registeredXPaths;
	}
	
	@Override
	public void addMatch(String channelName, String documentID) {
	    DocumentMatches matches = channelToMatches.get(channelName);
	    if(matches == null) {
	        matches = new DocumentMatches();
	    }
	    matches.addMatch(documentID);
	    channelToMatches.put(channelName, matches);
	}
	
	@Override
	public List<String> getMatches(String channelName) {
	    DocumentMatches matches = channelToMatches.get(channelName);
	    if(matches == null) {
	        return null;
        }
        return matches.getDocumentMatches();
	}
	
    @Override
	public Long getDocumentTimeStamp(String url) {
	    Integer docId = invUrls.get(url);
	    
	    if (docId == null)
	       return null;
	       
	    if(corpus.get(docId) != null) {
	        return corpus.get(docId).getTimeStamp();
	    }
	    return null;
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
    
    @Override
    public boolean hasSeen(String hash) {
        if(contentSeen.get(hash) != null) {
            return true;
        }
        else {
            return false;
        }
    }
    
    @Override
    public void insertSeen(String hash) {
        contentSeen.put(hash, 1);
    }
    
//    @Override
//    public void addHit(String keyword, Integer docId) {
//        
//    }

    @Override
    public int addUser(String firstname, String lastname, String username, String password) {
        int next = userMap.size();
        
        userMap.put(username, new User(next, firstname, lastname, username, password));
        return next;
    }



    @Override
	public synchronized void addPageRankRecord(String url, String outLinks) {
		pageRankMap.put(url, outLinks);
	}
	@Override
	public synchronized String getPageRankRecord(String url) {
		return pageRankMap.get(url);
	}


	@Override
    public boolean getSessionForUser(String username, String password) {
        if (userMap.get(username) != null &&
            userMap.get(username).getPassword().equals(password))
            return true;
        else
            return false;
    }
    
    @Override
    public String getUserFullName(String username, String password) {
        if(userMap.get(username) != null &&
            userMap.get(username).getPassword().equals(password)) {
                return new String(userMap.get(username).getFirstName() + " " + userMap.get(username).getLastName());
            }
        return "";
    }
	
}

