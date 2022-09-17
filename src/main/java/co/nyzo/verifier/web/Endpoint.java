package co.nyzo.verifier.web;

import java.util.Objects;

public class Endpoint {

    private String path;
    private HttpMethod method;
    private String host;

    public Endpoint(String path) {
        this.path = path;
        this.method = HttpMethod.Get;
        this.host = "";
    }

    public Endpoint(String path, HttpMethod method) {
        this.path = path;
        this.method = method;
        this.host = "";
    }

    public Endpoint(String path, HttpMethod method, String host) {
        this.path = path;
        this.method = method;
        this.host = host;
    }

    public String getPath() {
        return path;
    }

    public HttpMethod getMethod() {
        return method;
    }

    public String getHost() {
        return host;
    }

    public Endpoint getParentEndpoint() {
        int lastPathSeparatorIndex = path.lastIndexOf("/");
        String path = lastPathSeparatorIndex > 0 ? this.path.substring(0, lastPathSeparatorIndex) : "/";
        return new Endpoint(path, method, host);
    }

    public Endpoint getEmptyHostEndpoint() {
        return new Endpoint(path, method, "");
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, method, host);
    }

    @Override
    public boolean equals(Object object) {

        boolean result;
        if (this == object) {
            result = true;
        } else if (!(object instanceof Endpoint)) {
            result = false;
        } else {
            Endpoint endpoint = (Endpoint) object;
            result = this.path.equals(endpoint.path) && this.method == endpoint.method &&
                    this.host.equals(endpoint.host);
        }

        return result;
    }
}
