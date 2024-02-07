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
package org.projectnessie.s3mock;

import com.google.common.base.Preconditions;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

final class AwsChunkedInputStream extends InputStream {
  private final InputStream input;
  private AwsChunkedState state = AwsChunkedState.EXPECT_HEADER;
  private int chunkLen;

  // https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-streaming.html#sigv4-chunked-body-definition

  // Interestingly, for a 1MB upload, the AWS sync client uses 'aws-chunked' content-encoding, but
  // the AWS async client does not.

  AwsChunkedInputStream(InputStream input) {
    this.input = input;
  }

  enum AwsChunkedState {
    EXPECT_HEADER,
    DATA,
    EXPECT_SEPARATOR,
    EOF,
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    while (true) {
      switch (state) {
        case EOF:
          return -1;
        case EXPECT_HEADER:
          String header = readLine();
          if (header == null) {
            state = AwsChunkedState.EOF;
            break;
          }
          String[] parts = header.split(";");
          chunkLen = Integer.parseInt(parts[0], 16);
          if (chunkLen == 0) {
            state = AwsChunkedState.EOF;
            break;
          }
          // TODO verify 'chunk-signature'
          state = AwsChunkedState.DATA;
          break;
        case DATA:
          if (chunkLen == 0) {
            state = AwsChunkedState.EXPECT_SEPARATOR;
            break;
          }
          len = Math.min(chunkLen, len);
          if (len == 0) {
            return 0;
          }
          int rd = input.read(b, off, len);
          if (rd < 0) {
            state = AwsChunkedState.EOF;
            return -1;
          }
          chunkLen -= rd;
          return rd;
        case EXPECT_SEPARATOR:
          String sep = readLine();
          if (sep == null) {
            state = AwsChunkedState.EOF;
            break;
          }
          Preconditions.checkState(
              sep.isEmpty(), "Expecting empty separator line, but got '%s'", sep);
          state = AwsChunkedState.EXPECT_HEADER;
          break;
        default:
          throw new IllegalStateException();
      }
    }
  }

  @Override
  public int read() throws IOException {
    byte[] buf = new byte[1];
    int r = read(buf, 0, 1);
    if (r < 0) {
      return r;
    }
    return ((int) buf[0]) & 0xff;
  }

  private String readLine() throws IOException {
    StringBuilder line = new StringBuilder();
    while (true) {
      int c = input.read();
      if (c == -1) {
        if (line.length() == 0) {
          // End of stream "marker"
          return null;
        }
        throw new EOFException();
      }
      if (c == 13) {
        c = input.read();
        if (c == -1) {
          throw new EOFException();
        }
        if (c == 10) {
          return line.toString();
        } else {
          throw new IllegalArgumentException("Illegal CR-LF sequence");
        }
      }
      line.append((char) c);
    }
  }
}
