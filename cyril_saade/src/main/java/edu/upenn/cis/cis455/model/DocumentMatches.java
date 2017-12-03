package edu.upenn.cis.cis455.model;

import java.io.Serializable;
import java.util.List;
import java.util.ArrayList;

public class DocumentMatches implements Serializable {
    List<String> matchedDocuments;
    
    public DocumentMatches() {
        matchedDocuments = new ArrayList<String>();
    }
    
    public void addMatch(String name) {
        if(!matchedDocuments.contains(name)) // prevents having duplicate documents in match
            matchedDocuments.add(name);
    }
    
    public List<String> getDocumentMatches() {
        return matchedDocuments;
    }
}
