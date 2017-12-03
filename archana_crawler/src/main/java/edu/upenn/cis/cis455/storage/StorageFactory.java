package edu.upenn.cis.cis455.storage;

public class StorageFactory {
	static DBWrapper singletonDb = null;
    public static StorageInterface getDatabaseInstance(String directory) {
        if(singletonDb == null)
        	singletonDb = new DBWrapper(directory);
    	return singletonDb; 
    }
    public static StorageInterface getDatabaseInstance()
    {
    	return singletonDb;  //Will return NULL if called 
    						//before the construtor with the parameters
    }
}
