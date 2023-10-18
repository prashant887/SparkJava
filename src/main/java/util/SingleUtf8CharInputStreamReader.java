package util;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;

public class SingleUtf8CharInputStreamReader extends Reader {

    private final InputStream input;

    public SingleUtf8CharInputStreamReader(InputStream input) {
        this.input = input;
    }

    @Override
    public int read(char[] cbuf, int offset, int length) throws IOException {
        if (length < 1) {
            return 0;
        }

        int first = input.read();
       // System.out.println(" SingleUtf8CharInputStreamReader : First "+first +" char "+(char)first + " offset "+offset+" length "+length);
        if (first < 0) {
            return -1;
        }
        if (first < 0x80) {
            // System.out.println(" SingleUtf8CharInputStreamReader 0x80");
            cbuf[offset] = (char) first;
           // System.out.println(cbuf);

            return 1;
        }

        int size = aditionalBytes(first);
        byte[] bytes = new byte[size + 1];
        if (input.read(bytes, 1, size) < size) {
            return -1;
        }
        bytes[0] = (byte) first;
        String s = new String(bytes, StandardCharsets.UTF_8);
       // System.out.println(" SingleUtf8CharInputStreamReader S:"+s +" @ "+s.charAt(0));
        cbuf[offset] = s.charAt(0);
        return 1;
    }

    @Override
    public void close() throws IOException {
        input.close();

    }

    private static int aditionalBytes(int first) {
        //System.out.println(" SingleUtf8CharInputStreamReader aditionalBytes"+first);
        if (first < 0xe0) {
            return 1;
        }
        if (first < 0xf0) {
            return 2;
        }
        if (first < 0xf8) {
            return 3;
        }
        return 4;
    }
}
