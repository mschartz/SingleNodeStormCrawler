package edu.upenn.cis.cis455.xpathengine;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

import edu.upenn.cis.cis455.model.OccurrenceEvent;

public class XPathFSM {
    
    private Map<String, Integer> currentState; // mapping of docId->currentState of doc
    private List<State> states;
    private String xpathStr;
    
    private class State {
        public OccurrenceEvent.EventType type;
        public String value;
        public String operation;
        
        public State(OccurrenceEvent.EventType type, String value, String operation) {
            this.type = type;
            this.value = value;
            this.operation = operation;
        }
        
        public boolean isMatch(OccurrenceEvent event) {
            if(event.getType() == this.type && this.type == OccurrenceEvent.EventType.Text) {
                if(this.operation.equals("contains"))
                    return event.getValue().contains(this.value);
                else
                    return event.getValue().equals(this.value);
            }
            else {
                return (event.getType() == this.type && event.getValue().equals(this.value));
            }
        }
    }
    
    public XPathFSM(String xpath) {
        xpathStr = new String(xpath);
        currentState = new HashMap<String, Integer>();
        String[] levels = xpath.trim().split("/");
        states = new ArrayList<State>();
        
        
        //Pattern pattern = Pattern.compile(".*?[.*?]"); // TODO: maybe try to do it with regex?
        
        //Pattern pattern = Pattern.compile("<.*?>|\\|.*?\\|");
        
        // TODO: parse string into states (i.e. each state should be pushed to state list)
        for(int i=1; i<levels.length; i++) {
            String nodeName = new String();
            int strIndex;
            for(strIndex=0; strIndex<levels[i].length(); strIndex++) {
                if(levels[i].charAt(strIndex) == '[')
                    break;
                nodeName += levels[i].charAt(strIndex);
            }
            strIndex++;
            
            states.add(new State(OccurrenceEvent.EventType.ElementOpen, nodeName, "none"));
            if(strIndex >= levels[i].length() -1) {
                continue;
            }
            
            String command = "";
            while(levels[i].charAt(strIndex) != '(') {
                command+=levels[i].charAt(strIndex);
                strIndex++;
            }
            strIndex++;
            
            String parameters = "";
            if(command.trim().toLowerCase().equals("contains")) {
                command = "contains";
                boolean first = true;
                while(levels[i].charAt(strIndex) != ')' || first) {
                    if(levels[i].charAt(strIndex) == ')')
                    	first = false;
                    parameters += levels[i].charAt(strIndex);
                    strIndex++;
                }
                strIndex++;
                parameters = parameters.split(",")[1].trim().replace("\"", "");
            }
            else {
                command = "equals";
                strIndex++;
                while(levels[i].charAt(strIndex) != '=')
                    strIndex++;
                strIndex++;
                while(levels[i].charAt(strIndex) != ']') {
                    parameters += levels[i].charAt(strIndex);
                    strIndex++;
                }
                parameters = parameters.replace("\"", "");
                parameters = parameters.trim();
            }
            states.add(new State(OccurrenceEvent.EventType.Text, parameters, command));

        }
    }
    
    public String toString() {
        return new String(xpathStr);
    }
    
    /**
     * Moves to the next state if the current event matches the current state
     * Returns true if we reached the final state (i.e. it means that its a match)
     */
    public boolean transition(OccurrenceEvent event) {
        
        Integer stateOfDoc = currentState.get(event.getDocId());
        if(stateOfDoc == null) {
            stateOfDoc = new Integer(0);
            currentState.put(event.getDocId(), stateOfDoc);
        }
        
        if(stateOfDoc.intValue() == states.size())
            return true;

        if(event.getType() == OccurrenceEvent.EventType.ElementClose && stateOfDoc.intValue() > 0 && states.get(stateOfDoc-1).value.equals(event.getValue())) {
            stateOfDoc--;
            currentState.put(event.getDocId(), stateOfDoc);
            return false;
        }

            
        else if(states.get(stateOfDoc).isMatch(event) && event.getDepth() == stateOfDoc) {
            stateOfDoc++;
            currentState.put(event.getDocId(), stateOfDoc);
            return (stateOfDoc.intValue() == states.size());
        }
        
        currentState.put(event.getDocId(), stateOfDoc);
        return false;   
    }
}
