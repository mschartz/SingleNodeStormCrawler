package edu.upenn.cis.stormlite.bolt;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;
import org.ccil.cowan.tagsoup.jaxp.SAXParserImpl;

import edu.upenn.cis.cis455.crawler.Crawler;
import edu.upenn.cis.cis455.crawler.info.URLInfo;
import edu.upenn.cis.cis455.storage.StorageFactory;
import edu.upenn.cis.cis455.storage.StorageInterface;
import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.routers.IStreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;
import edu.upenn.cis.stormlite.tuple.Values;

public class DOMParserBolt implements IRichBolt {
	static Logger log = LogManager.getLogger(DOMParserBolt.class);

	String executorId = UUID.randomUUID().toString();
	Fields myFields = new Fields();
	String docId;
	String url;
	OutputCollector collector;
	
	final StorageInterface db;
	
	public DOMParserBolt
	() { 
		System.out.println("creating doc fetcher bolt");
		this.db = StorageFactory.getDatabaseInstance(Crawler.envPath);
	}

	@Override
	public String getExecutorId() {
		return executorId;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("docId", "url", "event", "name"));
		
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void execute(Tuple input) {
		System.out.println(getExecutorId() + "url "+ input.getStringByField("url"));
		System.out.println(getExecutorId() + "docId "+ input.getStringByField("docId"));
		System.out.println(getExecutorId() + "toParse "+ input.getStringByField("toParse"));
		this.docId = input.getStringByField("docId");
		this.url = input.getStringByField("url");
		String url = input.getStringByField("url");
		URLInfo urlObj = new URLInfo(url);
		//Open the file from DB
		String files = db.getDocument(urlObj.getHostName()+"/"+urlObj.getFilePath());
		
		InputStream stream = null;
		try {
			stream = new ByteArrayInputStream(files.getBytes(StandardCharsets.UTF_8.name()));
		} catch (UnsupportedEncodingException e) {
			
			e.printStackTrace();
		}
		
		//XML
		if(input.getStringByField("toParse").equals("false")) //XML file
		{
			
			SAXParserFactory factory = SAXParserFactory.newInstance();
			try {
	
			    //InputStream    xmlInput  = new FileInputStream(stream);
			    SAXParser      saxParser = factory.newSAXParser();
	
			    DefaultHandler handler   = new SaxHandler(this.docId, this.url);
			    saxParser.parse(stream, handler);
	
			} catch (Throwable err) {
			    err.printStackTrace ();
			}
		}
		else //HTML
		{
			DefaultHandler handler   = new SaxHandler(this.docId, this.url);
			try {
				SAXParserImpl.newInstance(null).parse(stream,handler);
			} catch (SAXException | IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	@Override
	public void prepare(Map<String, String> stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		
	}

	@Override
	public void setRouter(IStreamRouter router) {
		this.collector.setRouter(router);
		
	}

	@Override
	public Fields getSchema() {
		return myFields;
	}
	
	
	public class SaxHandler extends DefaultHandler {

		String docId;
		String url;
		public SaxHandler(String docId, String url)
		{
			this.docId = docId;
			this.url = url;
		}
		
		public void startDocument() throws SAXException {
//	        System.out.println("start document   : ");
	        
	        
	    }

	    public void endDocument() throws SAXException {
//	        System.out.println("end document     : ");
	    	collector.emit(new Values<Object>(this.docId,  this.url, "DocumentClosed", "doc" ));
	    	
	    }

	    public void startElement(String uri, String localName,
	        String qName, Attributes attributes)
	    throws SAXException {

//	        System.out.println("start element    : " + qName);
	        collector.emit(new Values<Object>(this.docId, this.url, "ElementOpen", qName ));
	    }

	    public void endElement(String uri, String localName, String qName)
	    throws SAXException {
//	        System.out.println("end element      : " + qName);
	        collector.emit(new Values<Object>(this.docId, this.url, "ElementClose", qName ));
	    }

	    public void characters(char ch[], int start, int length)
	    throws SAXException {
//	        System.out.println("start characters : " +
//	            new String(ch, start, length));
	        if((new String(ch, start, length) != null) || (!new String(ch, start, length).equals("")))
	        {
	        	collector.emit(new Values<Object>(this.docId, this.url, "Text", new String(ch, start, length) ));
	        }
	    }

	}
}
