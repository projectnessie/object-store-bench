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
package org.projectnessie.tools.objectstorebench.context;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.distribution.HistogramSnapshot;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.projectnessie.tools.objectstorebench.GetPut;
import org.projectnessie.tools.objectstorebench.GetPutOpts;
import org.projectnessie.tools.objectstorebench.time.MonotonicClock;

public class ExecutionContext {
  private final MonotonicClock monotonicClock;
  private final Map<String, ObjectsContext> objectsContexts = new HashMap<>();
  private final GetPut getPut;
  private final String bucket;
  private final MeterRegistry meterRegistry;
  private final GetPutOpts opts;
  private final Consumer<String> outputConsumer;
  private final Consumer<String> errorConsumer;

  public ExecutionContext(
      GetPut getPut,
      String bucket,
      GetPutOpts opts,
      Consumer<String> outputConsumer,
      Consumer<String> errorConsumer) {
    this.monotonicClock = MonotonicClock.newMonotonicClock();
    this.getPut = getPut;
    this.bucket = bucket;
    this.meterRegistry = new SimpleMeterRegistry();
    this.opts = opts;
    this.outputConsumer = outputConsumer;
    this.errorConsumer = errorConsumer;
  }

  public MonotonicClock monotonicClock() {
    return monotonicClock;
  }

  public GetPut getPut() {
    return getPut;
  }

  public String bucket() {
    return bucket;
  }

  public MeterRegistry meterRegistry() {
    return meterRegistry;
  }

  public GetPutOpts opts() {
    return opts;
  }

  public ObjectsContext getOrCreateObjectsContext(
      String objectContextName, NamingStrategy namingStrategy) {
    return objectsContexts.compute(
        objectContextName,
        (name, old) -> {
          if (old != null && old.namingStrategy() != namingStrategy) {
            throw new IllegalStateException(
                "Object context '"
                    + name
                    + "' already exists, but with a different naming strategy.");
          }
          return old != null ? old : new ObjectsContext(name, namingStrategy);
        });
  }

  public ObjectsContext getObjectsContext(String objectContextName) {
    return requireNonNull(objectsContexts.get(objectContextName), "Object context does not exist");
  }

  public DistributionSummary newDistributionSummary(String name) {
    DistributionSummary existing = meterRegistry.find(name).summary();
    if (existing != null) {
      throw new IllegalStateException("Meter '" + name + "' already registered");
    }

    return DistributionSummary.builder(name)
        .publishPercentileHistogram()
        .baseUnit("µs")
        .publishPercentiles(0.999d, 0.99d, 0.98d, 0.95d, 0.9d, 0.5d)
        .distributionStatisticExpiry(Duration.ofMinutes(2))
        .register(meterRegistry);
  }

  private static final long NANOS_PER_SECOND = SECONDS.toNanos(1);

  private int runningCnt;

  private static String errorsToString(Map<Integer, Integer> errors) {
    return errors.entrySet().stream()
        .sorted(Map.Entry.comparingByKey())
        .map(e -> e.getKey().toString() + ':' + e.getValue())
        .collect(Collectors.joining(", "));
  }

  private static final long KB = 1024L;
  private static final long MB = 1024L * KB;
  private static final long GB = 1024L * MB;

  private static String bytesToString(long bytes) {
    if (bytes < KB) {
      return bytes + "B";
    }
    if (bytes < MB) {
      return String.format("%.1fkB", (double) bytes / KB);
    }
    if (bytes < GB) {
      return String.format("%.1fMB", (double) bytes / MB);
    }
    return String.format("%.1fGB", (double) bytes / GB);
  }

  public void emitStats(
      long remainingNanos,
      HistogramSnapshot requestDuration,
      HistogramSnapshot timeToFirstByte,
      Map<Integer, Integer> errors,
      long bytes,
      long requests,
      int concurrent) {
    String remainingSecs =
        remainingNanos == Long.MAX_VALUE
            ? ""
            : Duration.ofSeconds(Math.round((double) remainingNanos / NANOS_PER_SECOND))
                .toString()
                .substring(2)
                .toLowerCase(Locale.ROOT);

    if ((runningCnt++ % 25) == 0) {
      printHeader();
    }

    emitStats(
        requestDuration,
        timeToFirstByte,
        errors,
        bytes,
        requests,
        concurrent,
        String.format("%8s", remainingSecs));
  }

  private void emitStats(
      HistogramSnapshot requestDuration,
      HistogramSnapshot timeToFirstByte,
      Map<Integer, Integer> errors,
      long bytes,
      long requests,
      int concurrent,
      String atLineStart) {
    outputConsumer.accept(
        String.format(
            "%s | @|yellow %8d |@‖"
                + " @|yellow %9.2f |@| @|yellow %9.2f |@| @|yellow %9.2f |@| @|yellow %9.2f |@| @|yellow %9.2f |@| @|yellow %9.2f |@| @|yellow %9.2f |@| @|yellow %9.2f |@‖"
                + " @|yellow %9.2f |@| @|yellow %9.2f |@| @|yellow %9.2f |@| @|yellow %9.2f |@| @|yellow %9.2f |@| @|yellow %9.2f |@| @|yellow %9.2f |@| @|yellow %9.2f |@‖"
                + " @|yellow %7d |@| @|yellow %8s |@| @|yellow %5d |@‖ @|red,bold %s|@%n",
            atLineStart,
            requestDuration.count(),
            requestDuration.mean(),
            requestDuration.max(),
            requestDuration.percentileValues()[0].value(), // p999
            requestDuration.percentileValues()[1].value(), // p99
            requestDuration.percentileValues()[2].value(), // p98
            requestDuration.percentileValues()[3].value(), // p95
            requestDuration.percentileValues()[4].value(), // p90
            requestDuration.percentileValues()[5].value(), // p50
            timeToFirstByte.mean(),
            timeToFirstByte.max(),
            timeToFirstByte.percentileValues()[0].value(), // p999
            timeToFirstByte.percentileValues()[1].value(), // p99
            timeToFirstByte.percentileValues()[2].value(), // p98
            timeToFirstByte.percentileValues()[3].value(), // p95
            timeToFirstByte.percentileValues()[4].value(), // p90
            timeToFirstByte.percentileValues()[5].value(), // p50
            concurrent,
            bytesToString(bytes),
            requests,
            errorsToString(errors)));
  }

  public void emitFinalStats(
      HistogramSnapshot requestDuration,
      HistogramSnapshot timeToFirstByte,
      Map<Integer, Integer> errors,
      long bytes,
      long requests,
      int concurrent) {
    if ((runningCnt++ % 25) == 0) {
      printHeader();
    }
    runningCnt = 0;
    emitStats(
        requestDuration,
        timeToFirstByte,
        errors,
        bytes,
        requests,
        concurrent,
        "  @|green,bold,underline FINAL|@ ");
  }

  private void printHeader() {
    outputConsumer.accept(
        String.format(
            """
    @|bold                     ‖ %-93s ‖ %s|@
    @|bold,underline          | %8s ‖ %9s | %9s | %9s | %9s | %9s | %9s | %9s | %9s ‖ %9s | %9s | %9s | %9s | %9s | %9s | %9s | %9s ‖ %7s | %8s | %5s ‖ %s|@
    """,
            "Request Duration [ms]",
            "Time to 1st bytes [ms]",
            "count",
            "mean",
            "max",
            "p999",
            "p99",
            "p98",
            "p95",
            "p90",
            "p50",
            "mean",
            "max",
            "p999",
            "p99",
            "p98",
            "p95",
            "p90",
            "p50",
            "concurr",
            "bytes",
            "req's",
            "errors"));
  }

  public void printError(String s) {
    errorConsumer.accept(s + '\n');
  }

  public void printInfo(String format) {
    outputConsumer.accept(format + '\n');
  }
}
