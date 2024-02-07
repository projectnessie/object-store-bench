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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class MonotonicClockImpl implements MonotonicClock {
  private final AtomicBoolean closed = new AtomicBoolean();

  MonotonicClockImpl() {
    MonotonicClockInternal.reference();
  }

  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      MonotonicClockInternal.dereference();
    }
  }

  public Clock newJavaClockAtUTC() {
    return newJavaClock(ZoneId.of("UTC"));
  }

  public Clock newJavaClock(ZoneId zoneId) {
    return new JavaClock(zoneId);
  }

  public TimerInstance newTimer() {
    long offset = instance().nanosSinceStarted();
    return () -> instance().nanosSinceStarted() - offset;
  }

  @Override
  public long wallClockMillis() {
    return instance().wallClockMillis();
  }

  @Override
  public long nanos() {
    return instance().nanosSinceStarted();
  }

  @Override
  public Instant instant() {
    return instance().wallClockInstant();
  }

  private static MonotonicClockInstance instance() {
    return MonotonicClockInternal.get();
  }

  static long nanosToMillis(long nanos) {
    return nanos / 1_000_000L;
  }

  private static final class MonotonicClockInstance {
    private static final Logger LOGGER = LoggerFactory.getLogger(MonotonicClockImpl.class);

    private Thread thread;
    private final Consumer<MonotonicClockInstance> onStop;
    private final long nanosOffset;
    private final long wallTimeOffset;
    private volatile long currentNanos;

    MonotonicClockInstance(Consumer<MonotonicClockInstance> onStop) {
      this.nanosOffset = System.nanoTime();
      this.wallTimeOffset = System.currentTimeMillis();
      this.onStop = onStop;
    }

    void start() {
      this.thread = new Thread(this::threadRun, "Monotonic clock");
      this.thread.start();
    }

    @SuppressWarnings("BusyWait")
    void threadRun() {
      LOGGER.info("Monotonic clock started");
      try {
        while (true) {
          if (Thread.interrupted()) {
            break;
          }

          tick();

          Thread.sleep(0, 200);
        }
      } catch (InterruptedException ie) {
        // exit
      } finally {
        LOGGER.info("Monotonic clock stopped");
        onStop.accept(this);
      }
    }

    void stop() {
      thread.interrupt();
    }

    private void tick() {
      currentNanos = System.nanoTime();
    }

    long nanosSinceStarted() {
      return currentNanos - nanosOffset;
    }

    long wallClockMillis() {
      return wallTimeOffset + nanosToMillis(nanosSinceStarted());
    }

    Instant wallClockInstant() {
      long nanos = nanosSinceStarted();

      long millis = TimeUnit.NANOSECONDS.toMillis(nanos);
      long nowMillis = wallTimeOffset + millis;

      long nanoAdjust = nanos % 1_000_000;
      nanoAdjust += (nowMillis % 1_000) * 1_000_000;

      long sec = TimeUnit.MILLISECONDS.toSeconds(nowMillis);

      return Instant.ofEpochSecond(sec, nanoAdjust);
    }
  }

  private final class JavaClock extends Clock {
    private final ZoneId zone;

    JavaClock(ZoneId zone) {
      this.zone = zone;
    }

    @Override
    public ZoneId getZone() {
      return zone;
    }

    @Override
    public Clock withZone(ZoneId zone) {
      return new JavaClock(zone);
    }

    @Override
    public long millis() {
      return instance().wallClockMillis();
    }

    @Override
    public Instant instant() {
      return instance().wallClockInstant();
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof JavaClock) {
        return zone.equals(((JavaClock) obj).zone);
      }
      return false;
    }

    @Override
    public int hashCode() {
      return zone.hashCode() + 1;
    }

    @Override
    public String toString() {
      return "JavaClock[" + zone + "]";
    }
  }

  private static final class MonotonicClockInternal {
    private static final AtomicReference<MonotonicClockInstance> thread = new AtomicReference<>();
    private static final AtomicInteger refCount = new AtomicInteger();

    private MonotonicClockInternal() {}

    static MonotonicClockInstance get() {
      return thread.get();
    }

    static void reference() {
      if (refCount.incrementAndGet() == 1) {
        MonotonicClockInstance inst = thread.get();
        if (inst == null) {
          inst = new MonotonicClockInstance(i -> thread.compareAndSet(i, null));
          if (thread.compareAndSet(null, inst)) {
            inst.start();
          }
        }
      }
    }

    static void dereference() {
      if (refCount.decrementAndGet() == 0) {
        MonotonicClockInstance inst = thread.getAndSet(null);
        if (inst != null) {
          inst.stop();
        }
      }
    }
  }
}
