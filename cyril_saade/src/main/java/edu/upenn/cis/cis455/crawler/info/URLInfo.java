package edu.upenn.cis.cis455.crawler.info;

import java.net.MalformedURLException;
import java.net.URL;

public class URLInfo {
	private String hostName;
	private int portNo;
	private String filePath;
	private boolean secure;
	private boolean isHtml;
	private String nextOperation;
	
	/**
	 * Constructor called with raw URL as input - parses URL to obtain host name and file path
	 */
	public URLInfo(String docURL) {
//	    try {
//			URL url = new URL(docURL);
//			secure = docURL.startsWith("https://");
//			filePath = 
//		} catch (MalformedURLException e) {
//			e.printStackTrace();
//		}
	    
		if(docURL == null || docURL.equals(""))
			return;
		docURL = docURL.trim();
		if(docURL.length() < 8 || (!docURL.startsWith("http://") && !docURL.startsWith("https://")))
		    return;
		secure = docURL.startsWith("https://");
		
		// Stripping off 'http://'
		if(secure)
		  docURL = docURL.replace("https://", "");
		else
		  docURL = docURL.replace("http://", "");
		
		// Stripping off www,
		docURL = docURL.replace("www.", "");

		int i = 0;
		while(i < docURL.length()){
			char c = docURL.charAt(i);
			if(c == '/')
				break;
			i++;
		}
		String address = docURL.substring(0,i);
		if(i == docURL.length())
			filePath = "/";
		else
			filePath = docURL.substring(i); //starts with '/'
		if(address.equals("/") || address.equals(""))
			return;
		if(address.indexOf(':') != -1){
			String[] comp = address.split(":",2);
			hostName = comp[0].trim();
			try{
				portNo = Integer.parseInt(comp[1].trim());
			}catch(NumberFormatException nfe){
				portNo = 80;
			}
		}else{
			hostName = address;
			portNo = 80;
		}
		nextOperation = new String("Robot.txt");
	}
	
	public void setNextOperation(String method) {
	    nextOperation = method;
	}
	
	public String getNextOperation() {
	    return nextOperation;
	}
	
	public URLInfo(String hostName, String filePath){
		this.hostName = hostName;
		this.filePath = filePath;
		this.portNo = 80;
	}
	
	public URLInfo(String hostName,int portNo,String filePath){
		this.hostName = hostName;
		this.portNo = portNo;
		this.filePath = filePath;
	}
	
	public boolean isSecure() {
	    return secure;
	}
	
	public String getHostName(){
		return hostName;
	}
	
	public void setHostName(String s){
		hostName = s;
	}
	
	public int getPortNo(){
		return portNo;
	}
	
	public void setPortNo(int p){
		portNo = p;
	}
	
	public String getFilePath(){
		return filePath;
	}
	
	public void setFilePath(String fp){
		filePath = fp;
	}
	
	public String toString() {
	    String fullUrl = new String();
	    fullUrl = (secure)?"https://":"http://";
	    fullUrl += hostName + filePath;
	    return fullUrl;
	}
	
	public void isHTML(boolean isHtml) {
	    this.isHtml = isHtml;
	}
	
	public boolean isHTML() {
	    return this.isHtml;
	}
	
}
