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
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;

public class OrcScan {
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    OrcFile.ReaderOptions options = OrcFile.readerOptions(conf);
    long rowCount = 0;
    for(String filename: args) {
      Reader reader = OrcFile.createReader(new Path(filename), options);
      TypeDescription schema = reader.getSchema();
      RecordReader rows = reader.rows();
      VectorizedRowBatch batch = schema.createRowBatch();
      while (rows.nextBatch(batch)) {
        rowCount += batch.size;
      }
      rows.close();
    }
    System.out.println("Rows read: " + rowCount);
  }
}
