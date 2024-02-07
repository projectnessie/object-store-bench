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

import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.projectnessie.tools.objectstorebench.Utils.bytesAsHex;
import static org.projectnessie.tools.objectstorebench.Utils.formatIso8601DateTimeNoZ;
import static org.projectnessie.tools.objectstorebench.Utils.formatIso8601DateTimeZulu;
import static org.projectnessie.tools.objectstorebench.Utils.parseIso8601DateTimeNoZ;
import static org.projectnessie.tools.objectstorebench.Utils.parseIso8601DateTimeZulu;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@ExtendWith(SoftAssertionsExtension.class)
public class TestUtils {
  @InjectSoftAssertions protected SoftAssertions soft;

  @ParameterizedTest
  @MethodSource
  public void iso8601DateTimeFormatter(
      TemporalAccessor temporalAccessor, String stringRepresentation) {
    soft.assertThat(formatIso8601DateTimeZulu(temporalAccessor)).isEqualTo(stringRepresentation);

    String suffixStripped = stringRepresentation.replaceFirst("\\(.*\\)Z", "$1");

    soft.assertThat(formatIso8601DateTimeNoZ(temporalAccessor)).isEqualTo(suffixStripped);
  }

  @ParameterizedTest
  @MethodSource
  public void iso8601DateTimeParser(
      TemporalAccessor temporalAccessor, String stringRepresentation) {
    soft.assertThat(parseIso8601DateTimeZulu(stringRepresentation))
        .extracting(
            ta -> ta.get(ChronoField.YEAR),
            ta -> ta.get(ChronoField.MONTH_OF_YEAR),
            ta -> ta.get(ChronoField.DAY_OF_MONTH),
            ta -> ta.get(ChronoField.DAY_OF_YEAR),
            ta -> ta.get(ChronoField.HOUR_OF_DAY),
            ta -> ta.get(ChronoField.MINUTE_OF_HOUR),
            ta -> ta.get(ChronoField.MINUTE_OF_DAY),
            ta -> ta.get(ChronoField.SECOND_OF_MINUTE),
            ta -> ta.get(ChronoField.NANO_OF_SECOND),
            ta -> ta.get(ChronoField.OFFSET_SECONDS))
        .containsExactly(
            temporalAccessor.get(ChronoField.YEAR),
            temporalAccessor.get(ChronoField.MONTH_OF_YEAR),
            temporalAccessor.get(ChronoField.DAY_OF_MONTH),
            temporalAccessor.get(ChronoField.DAY_OF_YEAR),
            temporalAccessor.get(ChronoField.HOUR_OF_DAY),
            temporalAccessor.get(ChronoField.MINUTE_OF_HOUR),
            temporalAccessor.get(ChronoField.MINUTE_OF_DAY),
            temporalAccessor.get(ChronoField.SECOND_OF_MINUTE),
            0,
            0);

    String suffixStripped = stringRepresentation.replaceFirst("\\(.*\\)Z", "$1");

    soft.assertThat(parseIso8601DateTimeNoZ(suffixStripped))
        .extracting(
            ta -> ta.get(ChronoField.YEAR),
            ta -> ta.get(ChronoField.MONTH_OF_YEAR),
            ta -> ta.get(ChronoField.DAY_OF_MONTH),
            ta -> ta.get(ChronoField.DAY_OF_YEAR),
            ta -> ta.get(ChronoField.HOUR_OF_DAY),
            ta -> ta.get(ChronoField.MINUTE_OF_HOUR),
            ta -> ta.get(ChronoField.MINUTE_OF_DAY),
            ta -> ta.get(ChronoField.SECOND_OF_MINUTE),
            ta -> ta.get(ChronoField.NANO_OF_SECOND),
            ta -> ta.get(ChronoField.OFFSET_SECONDS))
        .containsExactly(
            temporalAccessor.get(ChronoField.YEAR),
            temporalAccessor.get(ChronoField.MONTH_OF_YEAR),
            temporalAccessor.get(ChronoField.DAY_OF_MONTH),
            temporalAccessor.get(ChronoField.DAY_OF_YEAR),
            temporalAccessor.get(ChronoField.HOUR_OF_DAY),
            temporalAccessor.get(ChronoField.MINUTE_OF_HOUR),
            temporalAccessor.get(ChronoField.MINUTE_OF_DAY),
            temporalAccessor.get(ChronoField.SECOND_OF_MINUTE),
            0,
            0);
  }

  static Stream<Arguments> iso8601DateTimeFormatter() {
    return Stream.of(
        // "middle" of the day
        arguments(
            ZonedDateTime.of(2024, 1, 26, 10, 11, 42, 123123123, ZoneId.of("UTC")),
            "20240126T101142Z"),
        arguments(
            ZonedDateTime.of(
                2024, 1, 26, 10, 11, 42, 123123123, ZoneOffset.ofHours(-2).normalized()),
            "20240126T121142Z"),
        arguments(
            ZonedDateTime.of(
                2024, 1, 26, 10, 11, 42, 123123123, ZoneOffset.ofHours(2).normalized()),
            "20240126T081142Z"),
        // "midnight"
        arguments(
            ZonedDateTime.of(2024, 1, 26, 0, 0, 42, 123123123, ZoneId.of("UTC")),
            "20240126T000042Z"),
        arguments(
            ZonedDateTime.of(2024, 1, 26, 0, 0, 42, 123123123, ZoneOffset.ofHours(-2).normalized()),
            "20240126T020042Z"),
        arguments(
            ZonedDateTime.of(2024, 1, 26, 0, 0, 42, 123123123, ZoneOffset.ofHours(2).normalized()),
            "20240125T220042Z"),
        // LocalDateTime has no offset/zone - we just assume Z here
        arguments(LocalDateTime.of(2024, 1, 26, 10, 11, 42, 123123123), "20240126T101142Z"));
  }

  static Stream<Arguments> iso8601DateTimeParser() {
    return Stream.of(
        // "middle" of the day (zones/offsets do not matter here)
        arguments(
            ZonedDateTime.of(2024, 1, 26, 10, 11, 42, 0, ZoneId.of("UTC")), "20240126T101142Z"),
        arguments(
            ZonedDateTime.of(2024, 1, 26, 10, 11, 42, 0, ZoneOffset.ofHours(-2).normalized()),
            "20240126T101142Z"),
        arguments(
            ZonedDateTime.of(2024, 1, 26, 10, 11, 42, 0, ZoneOffset.ofHours(2).normalized()),
            "20240126T101142Z"),
        // "midnight" (zones/offsets do not matter here)
        arguments(ZonedDateTime.of(2024, 1, 26, 0, 0, 42, 0, ZoneId.of("UTC")), "20240126T000042Z"),
        arguments(
            ZonedDateTime.of(2024, 1, 26, 0, 0, 42, 0, ZoneOffset.ofHours(-2).normalized()),
            "20240126T000042Z"),
        arguments(
            ZonedDateTime.of(2024, 1, 26, 0, 0, 42, 0, ZoneOffset.ofHours(2).normalized()),
            "20240126T000042Z"));
  }

  @ParameterizedTest
  @MethodSource
  public void hexString(String str, byte[] bytes) {
    soft.assertThat(bytesAsHex(bytes)).isEqualTo(str);

    StringBuilder sb = new StringBuilder("foo");
    bytesAsHex(bytes, sb);
    sb.append("bar");
    soft.assertThat(sb.toString()).isEqualTo("foo" + str + "bar");
  }

  static Stream<Arguments> hexString() {
    return Stream.of(
        arguments("", new byte[0]),
        arguments("ab", new byte[] {(byte) 0xab}),
        arguments("deadbeef", new byte[] {(byte) 0xde, (byte) 0xad, (byte) 0xbe, (byte) 0xef}));
  }
}
