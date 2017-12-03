package edu.upenn.cis.cis455.model;

import java.io.Serializable;
import java.time.Instant;

public class CorpusEntry implements Serializable {
    Integer docId;
    String content;
    Long timeStamp;
    
    public Integer getDocId() {
        return docId;
    }
    
    public Long getTimeStamp() {
        return timeStamp;
    }
    
    public String getContent() {
        return content;
    }
    
    public CorpusEntry(Integer docId, String content) {
        this.docId = docId;
        this.content = content;
        this.timeStamp = new Long(Instant.now().toEpochMilli());
    }
    
    @Override
    public int hashCode() {
        return docId.hashCode();
    }
    
    @Override
    public boolean equals(Object o) {
        if (!(o instanceof CorpusEntry))
            return false;
            
        return ((CorpusEntry)o).docId.equals(docId);
    }
}
