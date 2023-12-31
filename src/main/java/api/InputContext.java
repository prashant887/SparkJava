package api;

public class InputContext {

    private final String bundleId;
    private final String filePath;

    public InputContext(String bundleId, String filePath) {
        this.bundleId = bundleId;
        this.filePath = filePath;
    }

    /**
     * @return the bundle id of the uploaded file as generated by the PhoneHome Platform. It is unique for each uploaded bundle/file.
     */
    public String getBundleId() {
        return this.bundleId;
    }

    /**
     * @return the file name of the bundle e.g. Kafka partition and offset
     */
    public String getFilePath() {
        return this.filePath;
    }

    @Override
    public String toString() {
        return "InputContext [bundleId=" + bundleId + ", filePath=" + filePath + "]";
    }
}
