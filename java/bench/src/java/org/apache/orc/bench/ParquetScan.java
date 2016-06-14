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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.parquet.read.DataWritableReadSupport;
import org.apache.hadoop.hive.ql.io.parquet.read.ParquetRecordReaderWrapper;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.parquet.hadoop.ParquetInputFormat;

public class ParquetScan {
  public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf();
    long rowCount = 0;
    ParquetInputFormat<ArrayWritable> inputFormat =
        new ParquetInputFormat<>(DataWritableReadSupport.class);

    NullWritable nada = NullWritable.get();
    for(String filename: args) {
      FileSplit split = new FileSplit(new Path(filename), 0, Long.MAX_VALUE,
          new String[]{});
      RecordReader<NullWritable,ArrayWritable> recordReader =
          new ParquetRecordReaderWrapper(inputFormat, split, conf,
              Reporter.NULL);
      ArrayWritable value = recordReader.createValue();
      while (recordReader.next(nada, value)) {
        rowCount += 1;
      }
      recordReader.close();
    }
    System.out.println("Rows read: " + rowCount);
  }
}
