package co.nyzo.verifier;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import co.nyzo.verifier.util.LogUtil;

public class DataAccumulatorPingVersionInfoResult {
    public final long created = System.currentTimeMillis(); 
    public AtomicBoolean isFinal = new AtomicBoolean(false); // if true the result will not change in content anymore
    public Map<String, Map<String, String>> result = new ConcurrentHashMap<>(); // identifier : {ip: version}

    public DataAccumulatorPingVersionInfoResult(){

    }
    
    public void addEntry(String identifier, String ip, String version){
        Map<String, String> existing = result.get(identifier);

        if (existing != null) {
            existing.put(ip, version);
        } else {
            Map<String, String> newValue = result.put(identifier, new ConcurrentHashMap<String, String>());
            newValue.put(ip, version);
        }

        Map<String, String> out = result.get(identifier);
        LogUtil.println("added: "+ out.toString());
    }

    public void finalize(){
        this.isFinal.set(true);
    }
}
