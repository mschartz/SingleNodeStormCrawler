package edu.upenn.cis.cis455.xpathengine;

import org.xml.sax.helpers.DefaultHandler;
import edu.upenn.cis.cis455.xpathengine.XPathEngineImpl;
/**
 * Implement this factory to produce your XPath engine
 * and SAX handler as necessary.  It may be called by
 * the test/grading infrastructure.
 * 
 * @author cis455
 *
 */
public class XPathEngineFactory {
//	static XPathEngine obj = null;
	public static XPathEngine getXPathEngine() {
//		if(obj == null)
//			obj = new XPathEngineImpl();
		return new XPathEngineImpl();
	}
	
	public static DefaultHandler getSAXHandler() {
		return null;
	}
}
