package co.nyzo.verifier.util;

import java.util.Calendar;

public final class CalendarUtil {
    public static long calculateDayDifference(Calendar calendar1, Calendar calendar2) {
        long millisDiff = calendar2.getTimeInMillis() - calendar1.getTimeInMillis();
        long dayDifference = millisDiff / (24 * 60 * 60 * 1000); // Convert milliseconds to days
        return dayDifference;
    }
}
