package api;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public interface Extractor {
    List<Record> extract(InputStream fileStream, InputContext fileContext) throws ParseException, IOException;

}
