package org.example.timestamp;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoUnit;

/**
 * 1. StorageApiDynamicDestinationsTableRow.TableRowConverter.toMessage()에서 protobuf 값 생성
 * 2. org.apache.beam.sdk.io.gcp.bigquery.TableRowToStorageApiProto.singularFieldToProtoValue()에서 TIMESTAMP타입의 String 값 변환에 사용하는 코드 - 에러 발생
 */
public class Timestamp2 {
    private static final DateTimeFormatter DATETIME_SPACE_FORMATTER =
            new DateTimeFormatterBuilder()
                    .append(DateTimeFormatter.ISO_LOCAL_DATE)
                    .appendLiteral(' ')
                    .append(DateTimeFormatter.ISO_LOCAL_TIME)
                    .toFormatter()
                    .withZone(ZoneOffset.UTC);

    public static void main(String[] args) {
        System.out.println(DATETIME_SPACE_FORMATTER.parse("2020-01-31 23:58:00.123"));

        System.out.println(Instant.from(DATETIME_SPACE_FORMATTER.parse("2020-01-31 23:58:00.123")));

        System.out.println(ChronoUnit.MICROS.between(
                Instant.EPOCH, Instant.from(DATETIME_SPACE_FORMATTER.parse("2020-01-31 23:58:00.123"))));
    }
}
