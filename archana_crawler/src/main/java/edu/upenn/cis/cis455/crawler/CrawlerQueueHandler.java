//package edu.upenn.cis.cis455.crawler;
//
//import java.util.List;
//
//public class CrawlerQueueHandler {
//	
//	int crawledUrlIndex = 0;
//	String site = null;
//	List<String> links;
//	
//	public String getNextLink()
//	{
//		try {
//			if( crawledUrlIndex < links.size())
//			{
//				if(links != null)
//				{
//					return links.get(crawledUrlIndex++);
//				}
//			}
//			else //Have finished all links for a hostname
//			{
//				if(site != null)
//				{
//					Crawler.siteQueue.add(site);
//				}
//				if(!Crawler.siteQueue.isEmpty())
//				{
//					site = Crawler.siteQueue.take();
//					if(site != null)
//					{
//						crawledUrlIndex = 0;
//						links = Crawler.urlQueue.get(site);
//					}
//				}
//			}
//			
//			
//			
//			if(site == null)
//			{
//				site = Crawler.siteQueue.take();
//				crawledUrlIndex = 0;
//				links = Crawler.urlQueue.get(site);
//			}
//			if( crawledUrlIndex < links.size()) //There are more links to crawl
//			{
//				return links.get(crawledUrlIndex++);
//			}
//			else //Finished crawling all the links in the host
//			{
//				Crawler.siteQueue.add(site);
//				site = null;
//			}
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		}
//		
//	}
//}
