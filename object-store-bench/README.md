# Nessie Object Store Benchmarking tool

Tool to measure performance of an object store, running from a single machine.

## Benchmark scripting language

Benchmark behavior is controlled using a scripting language. It allows the following commands, similar to SQL.
Multiple statements are separated using a semicolon (`;`). Multi-line comments (`/* ... */`) and single-line
comments (`// ...` or `# ...` or the SQL-like `-- ...`) are supported.

### Script examples

```
// Create 100 objects of size 2 MB, but no more than 5 concurrent PUT operations
PUT
  100 OBJECTS
  MAX 5 CONCURRENT
  OF SIZE 2 MB;

// With high rates (way higher than in this example, hundreds or thousands of
// requests per second), it is best practice to add a warmup statement. This
// allows the JVM and the client connection pool to warm up so that the actual
// benchmark results are not skewed, because histograms need quite some time to
// "fade out" the high request durations of the initial requests.  
GET
  5 OBJECTS
  AT RATE 10 PER SECOND
  WARMUP 20 SECONDS
  RUNTIME 30 SECONDS;

// Run GET operations across 5 of the previously created objects at the rate of
// 10 operations per second, runs for 1 minute.
GET
  5 OBJECTS
  AT RATE 10 PER SECOND
  RUNTIME 1 MINUTE;

// Delete all previously created objects.
DELETE;
```

The same script can be written as a 3-line script, ...

```
PUT 100 OBJECTS MAX 5 CONCURRENT OF SIZE 2 MB;
GET 5 OBJECTS AT RATE 10 PER SECOND RUNTIME 1 MINUTE;
DELETE;
```

or as a single line

```
PUT 100 OBJECTS MAX 5 CONCURRENT OF SIZE 2 MB; GET 5 OBJECTS AT RATE 10 PER SECOND RUNTIME 1 MINUTE; DELETE;
```

### Example running against Minio

Run the following command in a terminal:

```bash
podman run -ti -p 9000:9000 -p 9001:9001 --rm quay.io/minio/minio:latest server --console-address :9001 /data
```

Start the benchmarking tool in another terminal:

```bash
java -jar tools/object-store-bench/build/quarkus-app/quarkus-run.jar \
  --s3-access-key=minioadmin \
  --s3-secret-key=minioadmin \
  --base-uri=http://127.0.0.1:9000/ \
  --script='
// Create 100 objects of size 2 MB, but no more than 5 concurrent PUT operations
PUT
  100 OBJECTS
  MAX 5 CONCURRENT
  OF SIZE 2 MB;

// Run GET operations across 5 of the previously created objects at the rate of
// 10 operations per second, runs for 1 minute.
GET
  5 OBJECTS
  AT RATE 10 PER SECOND
  RUNTIME 1 MINUTE;

// Delete all previously created objects.
DELETE;
'
```

Scripts can also be stored on the file system and loaded using the `--input` command line option.

The above produces an output like the following. The meaning of the columns:

* `count`
* `Request duration` shows the mean, max and some percentiles of the whole request duration in milliseconds.
* `Time to 1st bytes` shows the mean, max and some percentiles of the whole request until the 1st block of bytes have
  been processed. The numbers do **not** reflect the literal 1st byte.
* `concurr` max concurrent requests
* `bytes` the net amount of bytes, populated for `GET` + `PUT` statements. If this value approaches the physical limit
  of the network interface, you do run into networking issues and are no longer benchmarking. You **must** keep this
  value below the **net** available bandwidth. With a 1GBit/s network interface and connection to your object store,
  make sure this value is _always_ below 70MB. For a 10Gbit/s network interface, the value must not exceed 700MB.
* `req's` the number of requests
* `errs` a map of `code:num` pairs, representing a HTTP status code and the number of occurrences. A code of `0` means
  that an non-HTTP error occurred, additional information printed to the console.

```
Initializing benchmark...
  implementation : AWS client / Apache 
  region         : eu-central-1 
  base uri       : http://127.0.0.1:9000/ 
  bucket         : bucket 
  access key     : minioadmin 
  virtual threads: true

Starting script statement: PUT 100 OBJECTS MAX 5 CONCURRENT OF SIZE 2 MB ...

Naming strategy 'CONSTANT PREFIX' uses seed "5c958dcc14655febb1c649508f8c43036ed482d2ddbe50f212ac70a8dea29630"
                    ‖ Request Duration [ms]                                                                         ‖ Time to 1st bytes [ms]
         |    count ‖      mean |       max |      p999 |       p99 |       p98 |       p95 |       p90 |       p50 ‖      mean |       max |      p999 |       p99 |       p98 |       p95 |       p90 |       p50 ‖ concurr |    bytes | req's ‖ errors
  FINAL  |      100 ‖     44.59 |    184.52 |    191.00 |    191.00 |    183.00 |     43.00 |     43.00 |     37.00 ‖      5.33 |     75.69 |     75.97 |     75.97 |     75.97 |      8.47 |      2.84 |      1.41 ‖       5 |  200.0MB |   100 ‖ 
Statement finished: PUT 100 OBJECTS MAX 5 CONCURRENT OF SIZE 2 MB


Starting script statement: GET 5 OBJECTS AT RATE 10 PER SECOND RUNTIME 1 MINUTE ...

                    ‖ Request Duration [ms]                                                                         ‖ Time to 1st bytes [ms]
         |    count ‖      mean |       max |      p999 |       p99 |       p98 |       p95 |       p90 |       p50 ‖      mean |       max |      p999 |       p99 |       p98 |       p95 |       p90 |       p50 ‖ concurr |    bytes | req's ‖ errors
     59s |       10 ‖      4.26 |     19.52 |     19.88 |     19.88 |     19.88 |     19.88 |      3.50 |      2.25 ‖      3.94 |     18.83 |     18.94 |     18.94 |     18.94 |     18.94 |      3.06 |      2.06 ‖       1 |   20.0MB |    10 ‖ 
     58s |       20 ‖      3.24 |     19.52 |     19.94 |     19.94 |     19.94 |      3.56 |      3.19 |      2.19 ‖      2.91 |     18.83 |     18.94 |     18.94 |     18.94 |      3.06 |      2.81 |      1.75 ‖       1 |   20.0MB |    10 ‖ 
... 
      2s |      580 ‖      2.27 |     19.52 |     19.94 |      3.56 |      3.44 |      2.94 |      2.81 |      2.19 ‖      1.83 |     18.83 |     18.97 |      3.34 |      3.09 |      2.72 |      2.47 |      1.66 ‖       1 |   20.0MB |    10 ‖ 
      1s |      590 ‖      2.27 |     19.52 |     19.94 |      3.56 |      3.44 |      2.94 |      2.81 |      2.19 ‖      1.82 |     18.83 |     18.97 |      3.34 |      3.09 |      2.72 |      2.47 |      1.66 ‖       1 |   20.0MB |    10 ‖ 
  FINAL  |      601 ‖      2.26 |     19.52 |     19.94 |      3.56 |      3.31 |      2.94 |      2.81 |      2.19 ‖      1.82 |     18.83 |     18.97 |      3.22 |      3.09 |      2.72 |      2.47 |      1.66 ‖       1 |   22.0MB |    11 ‖ 
Statement finished: GET 5 OBJECTS AT RATE 10 PER SECOND RUNTIME 1 MINUTE


Starting script statement: DELETE ...

                    ‖ Request Duration [ms]                                                                         ‖ Time to 1st bytes [ms]
         |    count ‖      mean |       max |      p999 |       p99 |       p98 |       p95 |       p90 |       p50 ‖      mean |       max |      p999 |       p99 |       p98 |       p95 |       p90 |       p50 ‖ concurr |    bytes | req's ‖ errors
  FINAL  |      100 ‖     28.28 |     65.65 |     67.50 |     67.50 |     67.50 |     63.50 |     63.50 |     22.50 ‖      0.00 |      0.00 |      0.00 |      0.00 |      0.00 |      0.00 |      0.00 |      0.00 ‖     100 |       0B |   100 ‖ 
Statement finished: DELETE
```

### `PUT` objects statement

Add objects to an object-context.

Syntax:

```
PUT
  [ n OBJECTS ]
  [ USING NAMING STRATEGY ns [WITH SEED seed ] ]
  [ AT RATE rv PER ru [ WARMUP d du] ]
  [ MAX c CONCURRENT ]
  [ RUNTIME d du ]
  [ IN CONTEXT "ctx" ]
  [ OF SIZE sv su ]
```

| object-count + runtime clauses                                                                       | object-count clause                     | runtime clause                             | neither      | 
|------------------------------------------------------------------------------------------------------|-----------------------------------------|--------------------------------------------|--------------|
| Create up to "object-count" objects, run for the specified duration, may overwrite existing objects. | Create the specified amount of objects. | Create objects for the specified duration. | Not allowed. |

### `REUSE` objects statement

Assume objects are present in the object store.

Syntax:

```
REUSE
  n OBJECTS
  USING NAMING STRATEGY ns WITH SEED seed
  [ IN CONTEXT "ctx" ]
```

Populating objects in an object store can be a time-consuming operation, so populating the object store once and using
the present objects is a better alternative.

For example, in the first benchmark run you issue the statement `PUT 10 OBJECTS;`. Watch for the line printed to the
console showing the `seed` value.

```
Starting script statement: PUT 10 OBJECTS ...

Naming strategy 'CONSTANT PREFIX' uses seed "a898a861f86004f471e4c4cc0f89212ba71f53c7d9ea215e0472b93b8834542e"
```

In following tool runs, instruct the benchmark tool to reuse those objects using the `REUSE` statement instead of
the `PUT` statement:

```
REUSE 10 OBJECTS
  USING NAMING STRATEGY CONSTANT PREFIX
  WITH SEED "a898a861f86004f471e4c4cc0f89212ba71f53c7d9ea215e0472b93b8834542e";

GET OBJECTS
  FOR 30 SECONDS
  AT RATE 10 PER SEONCD;
```

### `GET` objects statement

Retrieve objects in an object-context that were previously created using the `PUT` command for the same object-context.

Syntax:

```
GET
  [ n OBJECTS ]
  [ AT RATE rv PER ru [ WARMUP d du] ]
  [ MAX c CONCURRENT ]
  [ RUNTIME d du ]
  [ IN CONTEXT "ctx" ]
```

| object-count + runtime clauses                                   | object-count clause                       | runtime clause                                      | neither      | 
|------------------------------------------------------------------|-------------------------------------------|-----------------------------------------------------|--------------|
| Use a limited subset of objects, run for the specified duration. | Retrieve the specified number of objects. | Retrieve random objects until the duration elapsed. | Not allowed. |

### `DELETE` objects statement

Deletes objects from an object-context that were previously created using the `PUT` command for the same object-context.

Syntax:

```
DELETE
  [ n OBJECTS ]
  [ AT RATE rv PER ru [ WARMUP d du] ]
  [ MAX c CONCURRENT ]
  [ RUN FOR d du ]
  [ IN CONTEXT "ctx" ]
```

| object-count + runtime clauses                               | object-count clause | runtime clause                                                                           | neither                       | 
|--------------------------------------------------------------|---------------------|------------------------------------------------------------------------------------------|-------------------------------|
| Delete up to N objects, but stop when duration limit is hit. | Delete N objects.   | Delete as many objects as possible and exist, but no longer than the specified duration. | Delete all generated objects. |

### `USING NAMING STRATEGY ns [WITH SEED seed ]` (object naming strategy) clause

Specifies the naming strategy for objects.

* Using `RANDOM PREFIX` generates object names having a quite random prefix, avoids having objects in the same object
  store partition.
* Using `CONSTANT PREFIX` generates object names having a long-ish constant prefix.
* `WITH SEED seed` can be used to provide a deterministic seed for the random parts of the generated names.

### `n OBJECTS` (object count) clause

Specifies the number of objects, the behavior varies for different statements and with the combination of the
duration clause.

### `AT RATE rv PER ru [ WARMUP dv du] ` (rate limiting) clause

Limits the number of requests per time unit. Default is unlimited / as fast as possible. Mutually exclusive to the
concurrency limiting clause.

* `rv` is the rate value (e.g. `5`)
* `ru` is the rate unit (e.g. `SECOND`)

The optional `WARMUP` specifies the ramp up period, where `dv` is the duration value (e.g. `5`) and `du` is the duration
unit (e.g. `SECONDS`).

When used in combination with the concurrent request limiting clause, both limits apply.

### `MAX c CONCURRENT` (concurrency limiting) clause

Limits the number of concurrent requests. Default is unlimited / as many as needed.

When used in combination with the rate limiting clause, both limits apply.

### `RUNTIME dv du` (runtime) clause

Specifies the duration of the statement, the behavior varies for different statements and with the combination of the
object count clause.

* `dv` is the duration value (e.g. `5`)
* `du` is the duration unit (e.g. `SECONDS`)

### `IN CONTEXT "ctx"`  object-context clause

Defaults to `default` for the object-context name, if the clause is omitted.

### `OF SIZE sv su` size clause

Used to specify the size of objects. `sv` is a positive integer value, `su` is the size unit. Defaults to 1 MB, if
omitted.

* `sv` is the size value (e.g. `5`)
* `su` is the size unit (e.g. `MB`)

Examples:

* `500 BYTES`
* `10 MB` or `10 MEGABYTES`

## Object store client implementations

**The GCS and ADLS implementations are not properly tested, because neither has a suitable and working emulator!**

Available are these synchronous clients:

* AWS S3 using Java's `URLConnection` (synchronous), this is the default one being used, available explicitly via
  the `--implementation=awsUrlConnection` command line option.
* AWS S3 using Apache HTTP client (synchronous), available via the `--implementation=awsApache` command line option.
* Google Cloud Storage, available via the `--implementation=gcs` command line option.

Available are these asynchronous clients:

* AWS s3 using Netty (asynchronous), available via the `--implementation=awsNetty` command line option.
* Azure ADLS, available via the `--implementation=adls` command line option.

### S3 tested against Minio

All S3 clients have been tested against minio, both via integration tests and manually.

### No GCS emulator

Google provides no emulator for Google cloud storage, and the existing emulators are either not maintainer or uploads
don't work. GCS uploads are initiated using one HTTP request, which returns a `Location` header, which contains the URL
for the upload that the client must use. Problem is that a Fake server running inside a Docker container cannot infer
the dynamically mapped port for this test, because the initial HTTP request does not have a `Host` header. GCS is
tested manually from time to time.

### Azurite does not support ADLS Gen2

Azurite as of Feb 1st 2024 fails to parse the POST request when adding objects, error message in debug
log `Incoming URL doesn't match any of swagger defined request patterns.`, because it still does not support
ADLS Gen2.

## Things to consider when benchmarking

* Object store providers do throttle requests, globally, account-based and based on object store implementation details.
  This manifests as error codes (HTTP status codes) 429 and 503.
* Watch the throughput of the network interface. Exceeding the available bandwidth leads to incorrect results.
  A 1GBit/s interface allows for maybe 90MB/s net data transferred. This can be easily maxed out with 9 requests
  per second pulling a 10MB object!
* Make sure the network bandwidth is available not just on your network interface, but across the whole path to the
  object store and that the object store can handle the bandwidth and number of requests.
