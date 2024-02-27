package co.nyzo.verifier;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

import co.nyzo.verifier.messages.PingResponse;
import co.nyzo.verifier.util.IpUtil;
import co.nyzo.verifier.util.LogUtil;

// untested 

// not all data can be garnered from existing classes without introducing too much unrelated garbage in those classes, an "accumulator" was created, referencing it for accumulatory purposes may still be required 
// it may also create/initiate background procs, anything beyond mere extraction (and optionally transforming) within datadumper class probably belongs here 
public class DataAccumulator {
    private static final int _maxAvailablePingVersionInfoResults = 3;
    private static final long _pingThreadSleep = 60000L;

    public static Set<DataAccumulatorPingVersionInfoResult> PingVersionInfoResults = ConcurrentHashMap.newKeySet();
    public static DataAccumulatorPingVersionInfoResult LatestFinalizedPingVersionInfoResult = null;

    public DataAccumulator(){
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    sendPings();
                } catch(Exception e) {
                    LogUtil.println("[DataAccumulator][thread0]: unexpected exception, retrying.. " + e.toString() + "\r\n" + e.getStackTrace());
                }
                
                try {
					Thread.sleep(_pingThreadSleep); 
				} catch (InterruptedException e) {
                    LogUtil.println("[DataAccumulator][thread0]: Thread.sleep InterruptedException");
                }

                run();
            }
        }).start();
    }

    // A ping response contains the purported version for that node 
    public static void sendPings(){
        DataAccumulatorPingVersionInfoResult result = new DataAccumulatorPingVersionInfoResult();

        if(PingVersionInfoResults.size() >= _maxAvailablePingVersionInfoResults){
            long oldestTimestamp = Long.MAX_VALUE;
            DataAccumulatorPingVersionInfoResult oldestEntry = null;

            for(DataAccumulatorPingVersionInfoResult v : PingVersionInfoResults){
                if(!v.isFinal.get()){ // not the best finalization procede but should suffice
                    v.finalize();
                    LatestFinalizedPingVersionInfoResult = v;
                }

                if(v.created < oldestTimestamp) {
                    oldestEntry = v;
                    oldestTimestamp = v.created;
                }
            }

            PingVersionInfoResults.remove(oldestEntry);
        }

        PingVersionInfoResults.add(result);

        Map<ByteBuffer, Node> ipAddressNodeMap = NodeManager.getIpAddressToNodeMap();

        Set<ByteBuffer> ipAddresses = ipAddressNodeMap.keySet();
        Iterator<ByteBuffer> iterator = ipAddresses.iterator();

        while(iterator.hasNext()){
            ByteBuffer ipAddressBuffer = iterator.next();
            Node node = ipAddressNodeMap.get(ipAddressBuffer);

            Message pingMessage = new Message(MessageType.Ping200, null);
            Message.fetchTcp(IpUtil.addressAsString(ipAddressBuffer.array()), node.getPortTcp(), pingMessage, new MessageCallback() {
                @Override
                public void responseReceived(Message message){
                    if(message != null && message.getContent() instanceof PingResponse){
                        PingResponse response = (PingResponse)message.getContent();

                        LogUtil.println("res: " + response.toString());
                        LogUtil.println("origin res: " + message.getContent());

                        String[] messageSplit = response.toString().split("v=");

                        // Split result indicates an appropriate and expected amount of results 
                        if(messageSplit.length == 2){
                            String assumedVersionPart = messageSplit[messageSplit.length - 1];

                            // No major validation just a simple length check
                            if(assumedVersionPart.length() < 10){
                                if(!result.isFinal.get()){
                                    result.addEntry(node.getIdentifierString(), IpUtil.addressAsString(ipAddressBuffer.array()), assumedVersionPart);
                                }
                            }
                        }
                    }
                }
            });
        }
    }

    // "available" pertaining to the fact we might not be contacting the entire set of ip addresses (namely queue) while we could do so (todo assert if true and/or make it so) cf sendPings 
    // "available" pertaining to the responses received so far for the last ping send round 
    public static KeyValuePair<DataAccumulatorPingVersionInfoResult, Set<DataAccumulatorPingVersionInfoResult>> getAvailableVersionInfo(){
        return new KeyValuePair<DataAccumulatorPingVersionInfoResult,Set<DataAccumulatorPingVersionInfoResult>>(LatestFinalizedPingVersionInfoResult, PingVersionInfoResults);
    }
}
