package co.nyzo.verifier;

public class DataDumpResult {
    public static final long timestamp = System.currentTimeMillis();
    public Object result = null;

    public DataDumpResult(Object data) {
        result = data;
    }
}
