package hadoop;

import org.apache.hadoop.conf.Configuration;

@FunctionalInterface
public interface HadoopFactory<T, E extends Exception> {

    T create(Configuration conf) throws E;

}
