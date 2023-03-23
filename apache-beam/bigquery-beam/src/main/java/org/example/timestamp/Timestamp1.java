package org.example.timestamp;

import org.joda.time.DateTimeZone;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;

public class Timestamp1 {
    private static final DateTimeFormatter DATETIME_PART = new DateTimeFormatterBuilder()
            .appendYear(4, 4)
            .appendLiteral('-')
            .appendMonthOfYear(2)
            .appendLiteral('-')
            .appendDayOfMonth(2)
            .appendLiteral(' ')
            .appendHourOfDay(2)
            .appendLiteral(':')
            .appendMinuteOfHour(2)
            .appendLiteral(':')
            .appendSecondOfMinute(2)
            .toFormatter()
            .withZoneUTC();

    private static final DateTimeFormatter BIGQUERY_TIMESTAMP_PRINTER = new DateTimeFormatterBuilder()
            .append(DATETIME_PART)
            .appendLiteral('.')
            .appendFractionOfSecond(3, 3)
            .appendLiteral(" UTC")
            .toFormatter();

    public static void main(String[] args) {
        System.out.println(createTimestamp());
        System.out.println(createTimestamp("2020-03-20T03:41:42.123Z"));
    }

    public static String createTimestamp() {
        return new Instant().toDateTime(DateTimeZone.UTC).toString(BIGQUERY_TIMESTAMP_PRINTER);
    }

    public static String createTimestamp(String value) {
        return Instant.parse(value).toDateTime(DateTimeZone.UTC).toString(BIGQUERY_TIMESTAMP_PRINTER);
    }
}
