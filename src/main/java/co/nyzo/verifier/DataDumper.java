package co.nyzo.verifier;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.xml.crypto.dsig.keyinfo.KeyValue;

import co.nyzo.verifier.util.LogUtil;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class DataDumper {
    public static final File dataDumpDirectory = new File("/var/www/dumps");

    public static final File meshParticipantsFile = new File(DataDumper.dataDumpDirectory, "nodes.json");

    public DataDumper(){
        new Thread(new Runnable() {
            @Override
            public void run() {
                dump();
                
                try {
					Thread.sleep(5000);
				} catch (InterruptedException e) {
                    LogUtil.println("[DataDumper]: Thread.sleep InterruptedException");
                }

                run();
            }
        }).start();
    }

    public static void dump(){
        LogUtil.println("[DataDumper][dump]: Starting dump...");


        if(!NodeManager.connectedToMesh()){
            LogUtil.println("[DataDumper][dump]: Skipping dump due to not being connected to the mesh yet");
            return;
        }

        Map<String, Map<String, Node>> meshParticipants = DataDumper.getMeshParticipants();
        
        _persist(meshParticipantsFile, meshParticipants);


        LogUtil.println("[DataDumper][dump]: Completed dump...");
    }

    // identifier : [ip address : node]
    public static Map<String, Map<String, Node>> getMeshParticipants() {
        Map<String, Map<String, Node>> result = new ConcurrentHashMap<>();

        // identifier byte[], is in cycle : identifier string with dashes
        Map<KeyValuePair<byte[], Boolean>, String> identifierMap = new ConcurrentHashMap<>();

        // incycle identifiers 
        Set<ByteBuffer> activeInCycleVerifiers = NodeManager.getActiveCycleIdentifiers();

        LogUtil.println("[DataDumper][dump]: " + activeInCycleVerifiers.size() + " active in cycle verifiers");
        
        activeInCycleVerifiers.forEach(i -> {
            KeyValuePair<byte[], Boolean> k = new KeyValuePair<byte[], Boolean>(i.array(), true);

            identifierMap.put(
                k, 
                ByteUtil.arrayAsStringWithDashes(k.getKey())  
            );
        });

        LogUtil.println("[DataDumper][dump]: " + identifierMap.keySet().size() + " entries in identifier map");

        // missing incycle identifiers 
        Set<ByteBuffer> missingInCycleVerifiersSet = NodeManager.getMissingInCycleVerifiersSet();
        
        LogUtil.println("[DataDumper][dump]: " + missingInCycleVerifiersSet.size() + " missing in cycle verifiers");

        missingInCycleVerifiersSet.forEach(i -> {
            for(KeyValuePair<byte[], Boolean> k : identifierMap.keySet()){
                if(Arrays.equals(k.getKey(), i.array())){
                    return;
                }
            }

            KeyValuePair<byte[], Boolean> k = new KeyValuePair<byte[], Boolean>(i.array(), true);

            identifierMap.put(
                k, 
                ByteUtil.arrayAsStringWithDashes(k.getKey())
            );
        });

        LogUtil.println("[DataDumper][dump]: " + identifierMap.keySet().size() + " entries in identifier map");

        // ip address node map
        Map<ByteBuffer, Node> ipAddressNodeMap = NodeManager.getIpAddressToNodeMap();
        
        LogUtil.println("[DataDumper][dump]: " + ipAddressNodeMap.keySet().size() + " entries in ip address node map");

        ipAddressNodeMap.values().forEach(n -> {
            for(KeyValuePair<byte[], Boolean> k : identifierMap.keySet()){
                if(Arrays.equals(k.getKey(), n.getIdentifier())) {
                    return;
                }

                KeyValuePair<byte[], Boolean> kvp = new KeyValuePair<byte[], Boolean>(n.getIdentifier(), null);
            
                identifierMap.put(
                    kvp,
                    ByteUtil.arrayAsStringWithDashes(kvp.getKey())
                );
            }
        });
        
        LogUtil.println("[DataDumper][dump]: " + identifierMap.keySet().size() + " entries in identifier map");

        // creating the result
        identifierMap.keySet().forEach(i -> {
            // the string representation of the current identifier (value in the map)
            String iv = identifierMap.get(i);

            // ip address bytebuffer
            for(ByteBuffer k : ipAddressNodeMap.keySet()){
                // ip address node
                Node v = ipAddressNodeMap.get(k);

                // set incycle boolean
                v.setInCycle(i.getValue());

                // node identifier
                byte[] vi = v.getIdentifier();

                // the kvp aligns with the identifier from the identifierMap
                if(Arrays.equals(i.getKey(), vi)) {
                    if(!result.keySet().contains(iv)) {
                        // there is no entry yet, we add a new kvp to the result
                        result.put(iv, new ConcurrentHashMap<String, Node>());
                    }

                    // we add the ip adress node map kvp to the result
                    result.get(iv).put(v.getIpAddressString(), v);
                }
            }

            // the identifier is known, but we don't have any nodes to include in the result
            if(!result.keySet().contains(iv)) {
                result.put(iv, new ConcurrentHashMap<>());
            }
        }); 

        // ret
        return result;
    }

    private static void _persist(File file, Object object){
        ObjectMapper objectMapper = new ObjectMapper();
        DataDumpResult result = new DataDumpResult(object);

        try {
            _persist(file, objectMapper.writeValueAsString(result));
        } catch (JsonProcessingException e){
            LogUtil.println("[DataDumper][_persist]: failed to convert to json");
        }
    }

    private static void _persist(File file, String json){
        File tempFile = new File(file.getAbsolutePath() + "_temp");
        tempFile.delete();
        BufferedWriter writer = null;

        try {
            writer = new BufferedWriter(new FileWriter(tempFile));
            writer.write(json);
        } catch (Exception e){
            LogUtil.println("[DataDumper][_persist]: Failed to write json to temp file with path " + tempFile.getAbsolutePath());
        }

        try {
            if(writer != null){
                writer.close();
            }
        } catch (Exception e){
            LogUtil.println("[DataDumper][_persist]: Failed to close file writer for temp file with path " + tempFile.getAbsolutePath());
        }

        try {
            Files.move(
                Paths.get(tempFile.getAbsolutePath()), 
                Paths.get(file.getAbsolutePath()), 
                StandardCopyOption.ATOMIC_MOVE, 
                StandardCopyOption.REPLACE_EXISTING
            );
        } catch (Exception e){
            LogUtil.println("[DataDumper][_persist]: Failed to move temp file to final file path " + file.getAbsolutePath());
        }
    }
}
