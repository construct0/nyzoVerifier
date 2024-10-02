package co.nyzo.verifier;

public class Version {

    private int version = 647;
    private int subVersion = 3;

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
