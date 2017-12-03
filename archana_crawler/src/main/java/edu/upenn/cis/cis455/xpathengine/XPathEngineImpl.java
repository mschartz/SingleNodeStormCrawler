package edu.upenn.cis.cis455.xpathengine;

import java.util.ArrayList;
import java.util.List;

import edu.upenn.cis.cis455.model.OccurrenceEvent;
import edu.upenn.cis.cis455.model.OccurrenceEvent.EventType;

 class State{
	String[] step;
	String[] test;
	boolean[] testContains;
	int length;
	boolean waitForText;
	boolean waitForClose;
	String waitingForNodeClose;
	int index;
	
	public State()
	{
		step = new String[1000];
		test = new String[1000];
		testContains = new boolean[1000];
		index = 0;
		length = 0;
		waitForText = false;
		waitForClose = false;
		waitingForNodeClose = "";
	}
	
}
public class XPathEngineImpl implements XPathEngine {

	String[] xPaths;
	boolean xPathMatch[] = null;
	List<State> states;
	List<String> xpath_list;
	
	public XPathEngineImpl()
	{
		states = new ArrayList<State>();
		xpath_list = new ArrayList<String>();
	}
	
	public void addFinalList(List<String> xpa)
	{
		xpath_list = xpa;
		this.xPathMatch = new boolean[xpath_list.size()];
		
		
		makeStates();
	}
	
	@Override
	//Will be called only once in the program
	public void setXPaths(String[] expressions) {
		
		xPaths = expressions;
		
		
	}

	public void startDoc()
	{
		for(int i = 0;i<xpath_list.size(); i++)
		{
			xPathMatch[i] = true;
		}
		
	}
	
	public boolean[] getBoolArray()
	{
		return xPathMatch;
	}
	
	@Override
	//Will be called for all XPaths
	public boolean isValid(int i) {
		String xPath = xPaths[i];
		//check if valid
		// check for starting with /
		if(xPath == null)
			return false;
		String exp = xPath.replaceAll(" ", ""); //Ignore all spaces
		// can be / or step or test
		if(exp.startsWith("/"))
		{
			String[] steps = exp.split("/");
			//check step 
			int j = 0;
			//step can have test in it
			for(String step:steps)
			{
				if((step == null) || (step.equals(""))) {  if (j == 0 ) continue; else return false; }
				j++;
				System.out.println("Validating part: "+step);
				if(step.contains("["))
				{
					if(step.contains("]"))
					{
						String test = new String(step);
								
						test = test.substring(test.indexOf("[") + 1);
						test = test.substring(0, test.indexOf("]"));
						
						//if test can be text() = "somestring"
						if(test.startsWith("text()=\""))
						{
							if(test.endsWith("\"")) //Valid
								{}
							else
							{
								System.out.println("Failed validation:Doesn't end with quote");
								return false;
							}
						}
						else if(test.startsWith("contains(text(),\"")) //or contains(text(), "somestring")
						{
							if(test.endsWith("\")"))  //Valid
								{}
							else
							{
								System.out.println("Failed validation:Doesn't end with quote");
								return false;
							}
						}
						else //Invalid starting text of test
						{
							System.out.println("Failed validation:Invalid starting text of test");
							return false;
						}
						
					}
					else //No ending "]"
					{
						System.out.println("Failed validation:No ending \"]\"");
						return false;
					}
				}
			}
		}
		else
			return false;
																					
		return true;
	}

	@Override
	public boolean[] evaluateEvent(OccurrenceEvent event) {
		//Given an occurrence event check with the XPaths
		event.getDocId();
		EventType ev = event.getType();
		String name = event.getValue();
		
		for(int i =0; i<xpath_list.size();i++)
		{
			State state = states.get(i);
			if(state.index >= state.length) continue; //Xpath matching has been completed
			
			if((state.index == 0) && (xPathMatch[i]==true)) //Very first event for the document
			{
				if(ev == EventType.ElementOpen)
				{
					//Name and test if any MUST match
					if(name.equals(state.step[state.index]))
					{
						if(state.test[state.index] == null) //simple
						{
							xPathMatch[i] = true; //continue matching
							state.index++;
						}
						else //Complex case
						{
							state.waitForText = true;
							
						}
					}
					else
					{
						//Document did not start with the root expected
						//Discard the xpath for this doc
						xPathMatch[i] = false;
					}
					
				}
				else
				{
					//Discard the xpath for this doc
					xPathMatch[i] = false;
				}
			}
			
			//Not first event
			else
			{
				
				
				if(state.waitForClose == true)
				{
					if((ev == EventType.ElementClose) && (name.equals(state.waitingForNodeClose)) )
					{
						//continue matching
						state.waitForClose = false;
					}
					else
					{
						continue; //No matching for this XPath until the close event comes through
					}
				}
				
				
				
				if(xPathMatch[i]==true ) //Continue matching only if true
				{
					if(state.waitForText == true)
					{
						state.waitForText = false;
						if(ev == EventType.Text) {
							if(state.test[state.index] != null)
							{
								//Must match for contains
								boolean matched = false;
								if(state.testContains[state.index] == true)
								{
									matched = name.contains(state.test[state.index]);
								}
								else
								{
									matched = name.equals(state.test[state.index]);
								}
								//Step and test matched
								if( matched == true)
								{
									//match
									state.index++;
									xPathMatch[i] = true;
								}
								else
								{
									if(state.index == 0)
									{
										//No need to wait
										xPathMatch[i] = false;
									}
									else
									{
										xPathMatch[i] = true;
										state.waitForClose = true;
										state.waitingForNodeClose = state.step[state.index];
									}
								}
							}
								
							
						}
						else {
							if(state.index == 0)
							{
								//No need to wait
								xPathMatch[i] = false;
							}
							else
							{
								xPathMatch[i] = true;
//								state.waitForText = true;
								//Wait until you get an event close
								state.waitForClose = true;
								state.waitingForNodeClose = state.step[state.index];
							}
						}
					}
					else //No wait for text
					{
						if((ev == EventType.ElementOpen) && (name == state.step[state.index]))
						{
							if(state.test[state.index] == null) //Simple case
							{
								state.index++;
								xPathMatch[i] = true;
							}
							else // complex
							{
								state.waitForText = true;
								xPathMatch[i] = true;
							}
						}
						else if((ev == EventType.ElementClose) && (name == state.step[(state.index)-1]))
						{
							//Previous element matched is closed
							//never match
							xPathMatch[i] = true;
						}
						else
						{
							//no Match
							state.waitForClose = true;
							state.waitingForNodeClose = name;
							xPathMatch[i] = true;
						}
					}
					
				}
			}
		}
		
		return xPathMatch;
	}
	
	public void makeStates()
	{
		
		for(String xpath:this.xpath_list)
		{
			State state = new State();
			state.index = 0;
			String[] steps = xpath.split("/");
			if(steps != null) {
				int index =0;
				for(int i=1; i <steps.length;i++)
				{	String step = steps[i];
					if(step != null)
					{
						if(!step.contains("[")) //No test
						{
							state.step[index] = step;
							state.test[index] = null;
						}
						else
						{
							state.step[index] = step.split("[")[0];
							String test = new String(step);
							
							test = test.substring(test.indexOf("[") + 1);
							test = test.substring(0, test.indexOf("]"));
							
							String node = new String(test);
							node = node.substring(test.indexOf("\"") + 1);
							node = node.substring(0, test.indexOf("\""));
							state.test[index] = node;
							
							if(test.contains("contains")) {
								state.testContains[index] = true;
							}
							else
								state.testContains[index] = true;
						}
						index++;
					}
				}
				state.length = index;
				states.add(state);
			}
		}
	}
    
}
