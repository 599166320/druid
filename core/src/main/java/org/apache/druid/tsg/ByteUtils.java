/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.tsg;

import static java.util.Objects.requireNonNull;

final class ByteUtils
{
  private ByteUtils()
  {
  }

  static boolean getBit(final long n, final int k)
  {
    if (k >= 64) {
      throw new IllegalArgumentException(String.format("Position `%d` out of long range.", k));
    }
    return ((n >> k) & 1) == 1;
  }

  static byte[] concat(
      final byte[] bytes1,
      final byte[] bytes2
  )
  {
    requireNonNull(bytes1);
    requireNonNull(bytes2);
    final int bytes1Length = bytes1.length;
    final int bytes2Length = bytes2.length;
    final byte[] output = new byte[bytes1Length + bytes2Length];
    System.arraycopy(bytes1, 0, output, 0, bytes1Length);
    System.arraycopy(bytes2, 0, output, bytes1Length, bytes2Length);
    return output;
  }
}
