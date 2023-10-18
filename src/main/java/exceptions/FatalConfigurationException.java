package exceptions;

import java.io.IOException;

public class FatalConfigurationException extends SystemException{
    private static final long serialVersionUID = -5110889094889489222L;

    public FatalConfigurationException(String message, Throwable cause) {
        super(message, cause);
    }

    public FatalConfigurationException(String message) {
        super(message);
    }

    public FatalConfigurationException(Throwable t) {
        super(t);
    }

    public static FatalConfigurationException missingResource(String pathOnClasspath) {
        return new FatalConfigurationException(
                "Embedded configuration resource was not found on the classpath at " + pathOnClasspath);
    }

    public static FatalConfigurationException classpathIoError(String pathOnClasspath, IOException e) {
        return new FatalConfigurationException(
                "Embedded configuration resource could not be read the classpath at " + pathOnClasspath, e);
    }
}
