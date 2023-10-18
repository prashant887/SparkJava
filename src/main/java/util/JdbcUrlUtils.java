package util;

import org.apache.commons.lang3.StringUtils;

import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Helper utils for seting up JDBC url
 */
public class JdbcUrlUtils {

    public static String addApplicationName(String jdbcUrl, String applicationName) {
        if (!StringUtils.isEmpty(applicationName)
                && jdbcUrl.toLowerCase().startsWith("jdbc:postgresql:")) { // tested to work only on postgres
            jdbcUrl += jdbcUrl.indexOf('?') > -1 ? "&" : "?";
            jdbcUrl += "ApplicationName=" + applicationName;
        }
        return jdbcUrl;
    }

    public static String addApplicationName(String jdbcUrl) {
        return addApplicationName(jdbcUrl, guessApplicationName());
    }

    public static String guessApplicationName() {
        String appName = "";
        try {
            appName = ManagementFactory.getRuntimeMXBean().getName();
            List<StackTraceElement> stackTraceElements = Arrays.asList(new Throwable().getStackTrace());

            // ignore this class name
            stackTraceElements = stackTraceElements.subList(1, stackTraceElements.size());
            if (!stackTraceElements.isEmpty()
                    && stackTraceElements.get(0).getClassName().equals(JdbcUrlUtils.class.getName())) {
                stackTraceElements = stackTraceElements.subList(1, stackTraceElements.size());
            }

            Set<String> classNames = new LinkedHashSet<>();
            String mainMethodClassName = "";
            for (StackTraceElement stackTraceElement : stackTraceElements) {
                String className = stackTraceElement.getClassName();
                String simpleClassName = className.substring(className.lastIndexOf('.') + 1);
                if (stackTraceElement.getMethodName().equalsIgnoreCase("main")) {
                    mainMethodClassName = simpleClassName;
                    break;
                } else {
                    classNames.add(simpleClassName);
                }
            }
            appName += "_" + StringUtils.join(classNames.stream().limit(2).toArray(), "_");
            appName += "_" + mainMethodClassName;
        } catch (Exception e) {
        }
        return appName;
    }
}
