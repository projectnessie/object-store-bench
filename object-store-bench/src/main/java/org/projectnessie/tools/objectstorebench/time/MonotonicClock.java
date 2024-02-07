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
package org.projectnessie.tools.objectstorebench.time;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;

/**
 * Provides a monotonically increasing clock that is "immune" against wall-clock drifts.
 *
 * <p>{@link #newTimer()} and {@link #nanos()} represent the "real" time since the creation of the
 * {@link MonotonicClock} instance.
 *
 * <p>Wall-clock-ish values retrieved via {@link #instant()}, {@link #newJavaClock(ZoneId)}, {@link
 * #newJavaClockAtUTC()} or {@link #wallClockMillis()} return timestamps a recently valid wall-clock
 * timestamp plus the time since then.
 *
 * <p>Changes to the system wall clock does not affect any time value returned by this class.
 */
public interface MonotonicClock extends AutoCloseable {
  static MonotonicClock newMonotonicClock() {
    return new MonotonicClockImpl();
  }

  // instances returning "real" time

  TimerInstance newTimer();

  long nanos();

  // wall-clock-ish functions

  Instant instant();

  Clock newJavaClockAtUTC();

  Clock newJavaClock(ZoneId zoneId);

  long wallClockMillis();
}
