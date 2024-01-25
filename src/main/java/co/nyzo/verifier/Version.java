package co.nyzo.verifier;

public class Version {

    private int version = 644;
    private int subVersion = 8;

    public Version(){

    }

    public Version(int version, int subVersion){
        this.version = version;
        this.subVersion = subVersion;
    }

    public int getVersion() {
        return version;
    }

    public int getSubVersion(){
        return subVersion;
    }
}
