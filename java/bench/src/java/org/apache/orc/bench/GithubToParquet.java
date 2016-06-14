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
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.orc.VectorToWritable;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat;
import org.apache.hadoop.hive.serde2.io.ParquetHiveRecord;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.orc.TypeDescription;

import java.util.Properties;

public class GithubToParquet {

  public static void main(String[] args) throws Exception {
    TypeDescription schema = TaxiToOrc.loadSchema("github.schema");
    VectorizedRowBatch batch = schema.createRowBatch();
    JobConf conf = new JobConf();
    conf.set("mapred.task.id", "attempt_0_0_m_0_0");
    conf.set("parquet.compression", TaxiToParquet.getCodec(args[1]));
    Path path = new Path(args[0]);
    Properties properties = AvroWriter.setHiveSchema(schema);
    MapredParquetOutputFormat format = new MapredParquetOutputFormat();
    FileSinkOperator.RecordWriter writer = format.getHiveRecordWriter(conf,
        path, ParquetHiveRecord.class, !"none".equals(args[1]), properties,
        Reporter.NULL);
    ParquetHiveRecord record = new ParquetHiveRecord();
    record.inspector =
        (StructObjectInspector) VectorToWritable.createObjectInspector(schema);
    for(String inFile: TaxiToOrc.sliceArray(args, 2)) {
      JsonReader reader = new JsonReader(new Path(inFile), conf, schema);
      while (reader.nextBatch(batch)) {
        for(int r=0; r < batch.size; ++r) {
          record.value = VectorToWritable.createValue(batch, r, schema,
              record.value);
          writer.write(record);
        }
      }
    }
    writer.close(true);
  }
}
