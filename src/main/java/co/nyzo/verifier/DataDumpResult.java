package co.nyzo.verifier;

public class DataDumpResult {
    public long generatedTimestamp = -1;
    public long creatorInitialisationTimestamp = -1;
    public String systemMessage = "";
    public String resultMessage = "";

    public Object result = null;

    public DataDumpResult(Object data, String msg, long initialisationTimestamp) {
        generatedTimestamp = System.currentTimeMillis();
        result = data;
        resultMessage = msg;
        creatorInitialisationTimestamp = initialisationTimestamp;
    }
}
