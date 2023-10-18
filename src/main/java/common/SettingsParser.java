package common;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import org.apache.commons.lang3.ArrayUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;


public class SettingsParser {

   // private static final String[] COMPOSITE_PROPERTIES = { "cluster", "environment" };

    private static final String[] COMPOSITE_PROPERTIES = { "cluster", "environment" };

    public static <T> void parse(String[] args, T settings) {
        args = handleCompositeOptions(args);


        JCommander jCommander = new JCommander();
        jCommander.addObject(settings);
        try {
            jCommander.parse(args);
        } catch (ParameterException e) {
            StringBuilder msg = new StringBuilder();
            msg.append("Parsing " + settings.getClass().getSimpleName() + " settings failed. "
                    + "Invalid arguments passed either from a command line or in a configuration/properties file. "
                    + "Details follow (note that in a file the argument names are without prefix '--'): ");
            msg.append(e.getMessage());
            msg.append('\n');
            jCommander.usage();
            throw new IllegalArgumentException(msg.toString());
        }




    }


    public static String[] mapToArray(Map<String, String> argsMap) {
        List<String> args = new ArrayList<>();
        for (Map.Entry<String, String> kv : argsMap.entrySet()) {
            if (kv.getValue().equalsIgnoreCase("true")) {
                args.add("--" + kv.getKey());
            } else if (kv.getValue().equalsIgnoreCase("false")) {
                // skip - use default which is always false
            } else {
                args.add("--" + kv.getKey());
                args.add(kv.getValue());
            }
        }
        return args.toArray(new String[args.size()]);
    }

    /**
     * Converts arguments array to map. Assumes option without value to be a boolean switch.
     * Only long names (--argName) supported.
     */
    static Map<String, String> arrayToMap(String[] args) {
        Map<String, String> argsMap = new LinkedHashMap<>();
        int i = 0;
        while (i < args.length) {
            String arg = args[i++]; // get argument and increment
            if (arg.startsWith("--")) {
                arg = arg.substring(2);
            } else {
                throw new IllegalArgumentException("Expected option (with long name) at position " + i + " but got " + arg);
            }

            String value;
            if (i >= args.length || args[i].startsWith("-")) {
                // it must be a boolean option since it's missing value
                value = "true";
            } else { // get value and increment
                value = args[i++];
            }
            argsMap.put(arg, value);
        }
        return argsMap;
    }

    private static String[] handleCompositeOptions(String[] args) {
        System.out.println("Args :"+Arrays.toString(args));
        for (String propertyName : COMPOSITE_PROPERTIES) {
            String optionName = "--" + propertyName;
            int optionIndex = ArrayUtils.indexOf(args, optionName);
            if (optionIndex != -1) {
                Map<String, String> argsMap = arrayToMap(args);
                String value = argsMap.get(propertyName);
                argsMap.remove(propertyName);
                Map<String, String> inferred = getDefaultProperties(propertyName + "." + value + ".cfg");
                // if there are duplicated inferred properties they should be overwritten.
                inferred.putAll(argsMap);
                args = mapToArray(inferred);
            }
        }
        return args;
    }

    private static Map<String, String> getDefaultProperties(String resourceFile) {

        String contextMessage = String.format(
                "Cannot parse default settings. " + "The file is supposed to be in the classpath with name %s. ",
                resourceFile);

        try (InputStream input = SettingsParser.class.getClassLoader().getResourceAsStream(resourceFile)) {
            if (input == null) {
                throw new IllegalArgumentException(contextMessage + "The file is missing or cannot be open.");
            }
            // load the configuration file
            Properties prop = new Properties();
            try {
                prop.load(input);
            } catch (IOException e) {
                throw new IllegalArgumentException(contextMessage + "Error while reading the file.", e);
            }
            @SuppressWarnings({ "unchecked", "rawtypes" }) // pretty sure it's <string, string> map
            Map<String, String> propertiesMap = new HashMap<>((Map) prop);
            return propertiesMap;

        } catch (IOException e) {
            throw new IllegalArgumentException(contextMessage + "Unable to read from configuration file.", e);
        }
    }

}
