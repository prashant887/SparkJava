package hcat;

public class HCatalogException extends Exception {
    private static final long serialVersionUID = 1L;

    public HCatalogException(String message) {
        super(message);
    }

    public HCatalogException(String message, Throwable cause) {
        super(message, cause);
    }
}