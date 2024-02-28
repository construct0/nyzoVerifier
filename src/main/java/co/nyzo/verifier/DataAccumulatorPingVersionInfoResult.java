package co.nyzo.verifier;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import co.nyzo.verifier.util.LogUtil;

public class DataAccumulatorPingVersionInfoResult {
    public final long created = System.currentTimeMillis(); 
    public AtomicBoolean isFinal = new AtomicBoolean(false); // if true the result will not change in content anymore
    public Map<String, Map<String, String>> result = new ConcurrentHashMap<>(); // identifier : {ip: version}

    public DataAccumulatorPingVersionInfoResult(){}
    
    public void addEntry(String identifier, String ip, String version){
        Map<String, String> existing = result.get(identifier);

        if (existing == null) {
            result.put(identifier, new ConcurrentHashMap<String, String>());
            existing = result.get(identifier);
        }

        existing.put(ip, version);

        Map<String, String> existingMapForIdentifier = result.get(identifier);
        ObjectMapper objectMapper = new ObjectMapper();

        try {
            LogUtil.println(identifier + ": " + objectMapper.writeValueAsString(existingMapForIdentifier));
        } catch (JsonProcessingException e){
            LogUtil.println("[DataAccumulatorPingVersionInfoResult][addEntry]: Could not parse json existing map for identifier");
        }
    }

    public void finalize(){
        LogUtil.println("[DataAccumulatorPingVersionInfoResult][" + created + "]: finalizing..");
        this.isFinal.set(true);
    }
}
