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
import org.apache.orc.TypeDescription;
import org.apache.orc.tools.FileDump;
import org.iq80.snappy.SnappyOutputStream;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.zip.GZIPOutputStream;

public class TaxiToJson {

  public enum CompressionKind {
    NONE(""),
    ZLIB(".gz"),
    SNAPPY(".snappy");

    CompressionKind(String extendsion) {
      this.extension = extendsion;
    }

    private final String extension;

    public String getExtension() {
      return extension;
    }

    public OutputStream create(OutputStream out) throws IOException {
      switch (this) {
        case NONE:
          return out;
        case ZLIB:
          return new GZIPOutputStream(out);
        case SNAPPY:
          return new SnappyOutputStream(out);
        default:
          throw new IllegalArgumentException("Unhandled kind " + this);
      }
    }
  }

  public static CompressionKind getCodec(String name) {
    if ("none".equals(name)) {
      return CompressionKind.NONE;
    } else if ("zlib".equals(name)) {
      return CompressionKind.ZLIB;
    } else if ("snappy".equals(name)) {
      return CompressionKind.SNAPPY;
    } throw new IllegalArgumentException("Unhnadled kind " + name);
  }

  public static void main(String[] args) throws Exception {
    TypeDescription schema = TaxiToOrc.loadSchema("nyc-taxi.schema");
    Path path = new Path(args[0]);
    VectorizedRowBatch batch = schema.createRowBatch();
    Configuration conf = new Configuration();
    Writer output = new OutputStreamWriter(getCodec(args[1])
        .create(path.getFileSystem(conf).create(path)));
    for(String inFile: TaxiToOrc.sliceArray(args, 2)) {
      CsvReader reader = new CsvReader(new Path(inFile), conf, schema);
      while (reader.nextBatch(batch)) {
        for(int r=0; r < batch.size; ++r) {
          FileDump.printRow(output, batch, schema, r);
          output.write("\n");
        }
      }
    }
    output.close();
  }
}
