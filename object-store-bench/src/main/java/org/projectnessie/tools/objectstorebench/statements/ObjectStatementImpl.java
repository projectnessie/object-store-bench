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
package org.projectnessie.tools.objectstorebench.statements;

import com.google.common.util.concurrent.RateLimiter;
import io.micrometer.core.instrument.DistributionSummary;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import org.projectnessie.tools.objectstorebench.RequestStats;
import org.projectnessie.tools.objectstorebench.context.ExecutionContext;
import org.projectnessie.tools.objectstorebench.time.MonotonicClock;

abstract class ObjectStatementImpl<SPEC extends ObjectStatementSpec> implements Statement {

  private static final int ACQUIRE_TIMEOUT = 30;
  protected final SPEC statementSpec;

  protected ObjectStatementImpl(SPEC statementSpec) {
    this.statementSpec = statementSpec;
  }

  interface Limiter {
    boolean acquire() throws InterruptedException;

    void operationFinished();
  }

  @SuppressWarnings("UnstableApiUsage")
  protected Limiter createLimiter() {
    if (statementSpec.rateLimit().isPresent()) {
      if (statementSpec.maxConcurrent().isPresent()) {
        int maxConcurrent = statementSpec.maxConcurrent().getAsInt();
        int rateLimit = statementSpec.rateLimit().get().perSecond();
        // Rate limiting AND max-concurrent
        return new Limiter() {
          // TODO add warmup parameter
          final RateLimiter rateLimiter =
              statementSpec.warmup().isPresent()
                  ? RateLimiter.create(rateLimit, statementSpec.warmup().get())
                  : RateLimiter.create(rateLimit);
          final Semaphore concurrency = new Semaphore(maxConcurrent);

          boolean rateAcquired;
          boolean concurrencyAcquired;

          @Override
          public boolean acquire() {
            boolean r = rateAcquired;
            if (!r) {
              r = rateLimiter.tryAcquire();
              if (r) {
                rateAcquired = true;
              }
            }
            boolean c = concurrencyAcquired;
            if (!c) {
              c = concurrency.tryAcquire();
              if (c) {
                concurrencyAcquired = true;
              }
            }
            if (c && r) {
              concurrencyAcquired = rateAcquired = false;
              return true;
            }
            Thread.onSpinWait();
            return false;
          }

          @Override
          public void operationFinished() {
            concurrency.release();
          }
        };
      } else {
        // Rate limiting
        int rateLimit = statementSpec.rateLimit().get().perSecond();
        return new Limiter() {
          final RateLimiter rateLimiter =
              statementSpec.warmup().isPresent()
                  ? RateLimiter.create(rateLimit, statementSpec.warmup().get())
                  : RateLimiter.create(rateLimit);

          @Override
          public boolean acquire() {
            return rateLimiter.tryAcquire(ACQUIRE_TIMEOUT, TimeUnit.MILLISECONDS);
          }

          @Override
          public void operationFinished() {}
        };
      }
    } else if (statementSpec.maxConcurrent().isPresent()) {
      // Max-concurrent
      int maxConcurrent = statementSpec.maxConcurrent().getAsInt();
      return new Limiter() {
        final Semaphore sem = new Semaphore(maxConcurrent);

        @Override
        public boolean acquire() throws InterruptedException {
          return sem.tryAcquire(30, TimeUnit.MILLISECONDS);
        }

        @Override
        public void operationFinished() {
          sem.release();
        }
      };
    } else if (statementSpec.numObjects().isPresent() || implicitlyLimited()) {
      // As fast as possible
      return new Limiter() {
        @Override
        public boolean acquire() {
          Thread.onSpinWait();
          return true;
        }

        @Override
        public void operationFinished() {}
      };
    } else {
      throw new IllegalArgumentException(
          "Must specify at least one of rate-limit or max-concurrency or number of objects");
    }
  }

  protected boolean implicitlyLimited() {
    return false;
  }

  @Override
  public void execute(ExecutionContext executionContext) {
    DistributionSummary requestDuration =
        executionContext.newDistributionSummary("request-duration");
    DistributionSummary timeToFirstByte =
        executionContext.newDistributionSummary("time-to-first-byte");
    AtomicReference<Map<Integer, Integer>> errors =
        new AtomicReference<>(new ConcurrentHashMap<>());

    LongAdder bytes = new LongAdder();
    LongAdder requests = new LongAdder();

    AtomicInteger totalMaxConcurrent = new AtomicInteger();
    AtomicInteger maxConcurrent = new AtomicInteger();
    AtomicInteger concurrent = new AtomicInteger();

    try (ErrorCombiner errorCombiner = new ErrorCombiner(executionContext::printError)) {
      Limiter limiter = createLimiter();

      Consumer<RequestStats> statsConsumer =
          stats -> {
            int status = stats.status();
            if (status == 200) {
              stats.firstByteMicros().ifPresent(micros -> timeToFirstByte.record(micros / 1000d));
              requestDuration.record(stats.durationMicros() / 1000d);
              stats.contentLength().ifPresent(bytes::add);
            } else {
              if (stats.failureMessage().isPresent()) {
                errorCombiner.push(stats.failureMessage().get());
              } else {
                stats
                    .failure()
                    .ifPresent(
                        thrown -> {
                          if (thrown instanceof CompletionException) {
                            thrown = thrown.getCause();
                          }
                          errorCombiner.push(thrown.toString());
                        });
              }
              errors.get().compute(status, (key, old) -> old != null ? old + 1 : 1);
            }
            requests.increment();
            limiter.operationFinished();
            concurrent.decrementAndGet();
          };

      @SuppressWarnings("resource")
      MonotonicClock clock = executionContext.monotonicClock();
      long nowNanos = clock.nanos();
      long statsInterval = executionContext.opts().statsInterval.toNanos();
      long nextPrint = nowNanos + statsInterval;
      StatementContext statementContext = buildStatementContext(executionContext).start(nowNanos);

      while (!statementContext.finished(nowNanos)) {
        if (limiter.acquire()) {
          int running = concurrent.incrementAndGet();
          maxConcurrent.getAndUpdate(v -> Math.max(v, running));

          startOperation(executionContext, statementContext)
              .toCompletableFuture()
              .whenComplete(
                  (stats, thrown) -> {
                    if (stats == null) {
                      stats =
                          RequestStats.builder()
                              .started(0L)
                              .durationMicros(1L)
                              .status(999)
                              .failure(thrown)
                              .build();
                    }
                    statsConsumer.accept(stats);
                  });
        }

        nowNanos = clock.nanos();
        if (nowNanos >= nextPrint) {
          errorCombiner.flush();
          nextPrint = nowNanos + statsInterval;
          long end = statementContext.endNanos();
          int current = maxConcurrent.getAndSet(0);
          totalMaxConcurrent.getAndUpdate(v -> Math.max(v, current));
          executionContext.emitStats(
              end == Long.MAX_VALUE ? Long.MAX_VALUE : (statementContext.endNanos() - nowNanos),
              requestDuration.takeSnapshot(),
              timeToFirstByte.takeSnapshot(),
              errors.getAndSet(new ConcurrentHashMap<>()),
              bytes.sumThenReset(),
              requests.sumThenReset(),
              current);
        }
      }

      while (concurrent.get() > 0) {
        Thread.onSpinWait();
      }
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } finally {
      try {
        int current = maxConcurrent.getAndSet(0);
        totalMaxConcurrent.getAndUpdate(v -> Math.max(v, current));
        executionContext.emitFinalStats(
            requestDuration.takeSnapshot(),
            timeToFirstByte.takeSnapshot(),
            errors.getAndSet(new ConcurrentHashMap<>()),
            bytes.sumThenReset(),
            requests.sumThenReset(),
            totalMaxConcurrent.getAndSet(0));
      } finally {
        executionContext.meterRegistry().remove(requestDuration);
        executionContext.meterRegistry().remove(timeToFirstByte);
      }
    }
  }

  protected abstract StatementContext buildStatementContext(ExecutionContext executionContext);

  protected abstract CompletionStage<RequestStats> startOperation(
      ExecutionContext executionContext, StatementContext statementContext);

  public interface StatementContext {
    default StatementContext start(long nowNanos) {
      return this;
    }

    String nextObject();

    long endNanos();

    boolean finished(long nowNanos);
  }
}
