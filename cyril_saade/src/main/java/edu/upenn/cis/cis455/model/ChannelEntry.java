package edu.upenn.cis.cis455.model;

import java.io.Serializable;
import java.util.List;
import java.util.ArrayList;

public class ChannelEntry implements Serializable {
    List<String> subscriptions;
    
    public ChannelEntry() {
        subscriptions = new ArrayList<String>();
    }
    
    public void addChannel(String name) {
        subscriptions.add(name);
    }
    
    public List<String> getSubscriptions() {
        return subscriptions;
    }
}
