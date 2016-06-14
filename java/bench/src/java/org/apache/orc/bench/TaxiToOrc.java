/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.orc.bench;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.orc.CompressionKind;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

public class TaxiToOrc {

  public static TypeDescription loadSchema(String name) throws IOException {
    InputStream in = TaxiToOrc.class.getClassLoader().getResourceAsStream(name);
    byte[] buffer= new byte[1 * 1024];
    int len = in.read(buffer);
    StringBuilder string = new StringBuilder();
    while (len > 0) {
      for(int i=0; i < len; ++i) {
        // strip out
        if (buffer[i] != '\n' && buffer[i] != ' ') {
          string.append((char) buffer[i]);
        }
      }
      len = in.read(buffer);
    }
    return TypeDescription.fromString(string.toString());
  }

  public static CompressionKind getCodec(String compression) {
    if ("none".equals(compression)) {
      return CompressionKind.NONE;
    } else if ("zlib".equals(compression)) {
      return CompressionKind.ZLIB;
    } else if ("snappy".equals(compression)) {
      return CompressionKind.SNAPPY;
    } else {
      throw new IllegalArgumentException("Unknown compression " + compression);
    }
  }

  public static Iterable<String> sliceArray(final String[] array,
                                            final int start) {
    return new Iterable<String>() {
      String[] values = array;
      int posn = start;

      @Override
      public Iterator<String> iterator() {
        return new Iterator<String>() {
          @Override
          public boolean hasNext() {
            return posn < values.length;
          }

          @Override
          public String next() {
            return values[posn++];
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException("No remove");
          }
        };
      }
    };
  }

  public static void main(String[] args) throws Exception {
    TypeDescription schema = loadSchema("nyc-taxi.schema");
    VectorizedRowBatch batch = schema.createRowBatch();
    Configuration conf = new Configuration();
    Writer writer = OrcFile.createWriter(new Path(args[0]),
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .compress(getCodec(args[1])));
    for(String inFile: sliceArray(args, 2)) {
      CsvReader reader = new CsvReader(new Path(inFile), conf, schema);
      while (reader.nextBatch(batch)) {
        writer.addRowBatch(batch);
      }
    }
    writer.close();
  }
}
