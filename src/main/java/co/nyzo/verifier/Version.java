package co.nyzo.verifier;

public class Version {

    private int version = 646;
    private int subVersion = 0;

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
