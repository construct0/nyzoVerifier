package co.nyzo.verifier;

public class DataDumpResult {
    public long generated = -1;
    public Object result = null;

    public DataDumpResult(Object data) {
        generated = System.currentTimeMillis();
        result = data;
    }
}
