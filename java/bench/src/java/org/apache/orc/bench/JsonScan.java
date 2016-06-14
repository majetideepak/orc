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

import com.google.gson.JsonStreamParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;

public class JsonScan {
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    OrcFile.ReaderOptions options = OrcFile.readerOptions(conf);
    long rowCount = 0;
    for(String filename: args) {
      Path path = new Path(filename);
      FileSystem fs = path.getFileSystem(conf);
      FSDataInputStream raw = fs.open(path);
      int lastDot = filename.lastIndexOf(".");
      InputStream input = raw;
      if (lastDot >= 0) {
        if (".gz".equals(filename.substring(lastDot))) {
          input = new GZIPInputStream(raw);
        }
      }
      JsonStreamParser parser =
          new JsonStreamParser(new InputStreamReader(input));
      while (parser.hasNext()) {
        parser.next();
        rowCount += 1;
      }
    }
    System.out.println("Rows read: " + rowCount);
  }
}
