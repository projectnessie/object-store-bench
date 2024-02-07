/*
 * Copyright (C) 2024 Dremio
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.projectnessie.tools.objectstorebench;

import static java.time.ZoneOffset.UTC;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.Locale;

public final class Utils {
  private Utils() {}

  public static final String EMPTY_SHA256 =
      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

  public static final DateTimeFormatter ISO8601_DATE_NO_Z_FORMATTER =
      new DateTimeFormatterBuilder()
          .appendValue(ChronoField.YEAR, 4)
          .appendValue(ChronoField.MONTH_OF_YEAR, 2)
          .appendValue(ChronoField.DAY_OF_MONTH, 2)
          .toFormatter(Locale.ROOT)
          .withZone(ZoneId.of("Z"));

  private static final DateTimeFormatter ISO8601_DATE_TIME_FORMATTER =
      new DateTimeFormatterBuilder()
          .appendValue(ChronoField.YEAR, 4)
          .appendValue(ChronoField.MONTH_OF_YEAR, 2)
          .appendValue(ChronoField.DAY_OF_MONTH, 2)
          .appendLiteral('T')
          .appendValue(ChronoField.HOUR_OF_DAY, 2)
          .appendValue(ChronoField.MINUTE_OF_HOUR, 2)
          .appendValue(ChronoField.SECOND_OF_MINUTE, 2)
          .appendOffset("+HHMMss", "Z")
          .toFormatter(Locale.ROOT)
          .withZone(ZoneId.of("Z"));

  /**
   * Formats the given temporal accessor using ISO-8601, using the pattern {@code yyyyMMddTHHmmssZ}.
   * The offset/zone of the temporal accessor is converted to UTC if necessary.
   */
  public static String formatIso8601DateTimeZulu(TemporalAccessor temporalAccessor) {
    return ISO8601_DATE_TIME_FORMATTER.format(toUTC(temporalAccessor));
  }

  /**
   * Parses an ISO-8601, using the pattern {@code yyyyMMddTHHmmssZ} (assume {@code
   * yyyyMMddTHHmmssX}), converting the timestamp to UTC if necessary.
   */
  public static TemporalAccessor parseIso8601DateTimeZulu(CharSequence iso8601DateTime) {
    return toUTC(ISO8601_DATE_TIME_FORMATTER.parse(iso8601DateTime));
  }

  /**
   * Formats the given temporal accessor using ISO-8601, using the pattern {@code yyyyMMddTHHmmss},
   * so like {@link #formatIso8601DateTimeZulu(TemporalAccessor)} but without the trailing {@code
   * Z}. The offset/zone of the temporal accessor is converted to UTC if necessary.
   */
  public static String formatIso8601DateTimeNoZ(TemporalAccessor temporalAccessor) {
    return ISO8601_DATE_TIME_FORMATTER.format(toUTC(temporalAccessor));
  }

  /**
   * Parses an ISO-8601, using the pattern {@code yyyyMMddTHHmmss}, so like {@link
   * #parseIso8601DateTimeZulu(CharSequence)}, assumes UTC.
   */
  public static TemporalAccessor parseIso8601DateTimeNoZ(CharSequence iso8601DateTime) {
    return toUTC(ISO8601_DATE_TIME_FORMATTER.parse(iso8601DateTime));
  }

  private static TemporalAccessor toUTC(TemporalAccessor temporalAccessor) {
    if (!temporalAccessor.isSupported(ChronoField.OFFSET_SECONDS)) {
      if (temporalAccessor instanceof LocalDateTime) {
        temporalAccessor = ZonedDateTime.of((LocalDateTime) temporalAccessor, UTC);
      }
    } else if (temporalAccessor.get(ChronoField.OFFSET_SECONDS) != 0) {
      temporalAccessor = Instant.from(temporalAccessor).atOffset(UTC);
    }
    return temporalAccessor;
  }

  public static String bytesAsHex(byte[] bytes) {
    StringBuilder sb = new StringBuilder(bytes.length * 2);
    bytesAsHex(bytes, sb);
    return sb.toString();
  }

  public static void bytesAsHex(byte[] bytes, StringBuilder target) {
    String hex = "0123456789abcdef";
    for (byte b : bytes) {
      target.append(hex.charAt((b >> 4) & 0xf)).append(hex.charAt(b & 0xf));
    }
  }
}
