package util;

import org.apache.commons.lang.time.DateUtils;

import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

public class DateUtil {
    private static final TimeZone UTC_TIME_ZONE = TimeZone.getTimeZone("UTC");

    /**
     * Returns the time in seconds (e.g. Date#getTime() / 1000) but also truncated to the day in UTC.
     * @see #truncateToDay(Date)
     */
    public static long truncateToDayInSeconds(Date date) {
        return truncateToDay(date).getTime() / 1000;
    }

    /**
     * Returns the time in seconds (e.g. Date#getTime() / 1000) but also truncated to the month in UTC.
     * @see #truncateToMonth(Date)
     */
    public static long truncateToMonthInSeconds(Date date) {
        return truncateToMonth(date).getTime() / 1000;
    }

    /**
     * Returns the date truncated to the day in UTC.
     * <p>
     * For example, if you had the date of 18 April 2016 14:57:01.231, it would return 18 April 2016 00:00:00.000.
     */
    public static Date truncateToDay(Date date) {
        return truncateToPeriod(date, Calendar.DATE);
    }

    /**
     * Returns the date truncated to the month in UTC.
     * <p>
     * For example, if you had the date of 18 April 2016 14:57:01.231, it would return 1 April 2016 00:00:00.000.
     */
    public static Date truncateToMonth(Date date) {
        return truncateToPeriod(date, Calendar.MONTH);
    }

    private static Date truncateToPeriod(Date date, int field) {
        Calendar utcDate = Calendar.getInstance(UTC_TIME_ZONE);
        utcDate.setTime(date);
        Date period = DateUtils.truncate(utcDate, field).getTime();
        return period;
    }
}
