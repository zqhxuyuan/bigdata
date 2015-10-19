package uom.msc.debs;

public class TestQuery {
    public String queryId;
    public String query;
    public int cudaDeviceId;
    
    public TestQuery(String queryId, String query, int cudaDeviceId) {
        this.queryId = queryId;
        this.query = query;
        this.cudaDeviceId = cudaDeviceId;
    }
}
