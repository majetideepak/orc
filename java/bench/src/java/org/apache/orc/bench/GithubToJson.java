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

import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.zip.GZIPOutputStream;

public class GithubToJson {

  public static void main(String[] args) throws Exception {
    TypeDescription schema = TaxiToOrc.loadSchema("github.schema");
    Path path = new Path(args[0]);
    VectorizedRowBatch batch = schema.createRowBatch();
    Configuration conf = new Configuration();
    Writer output = new OutputStreamWriter(TaxiToJson.getCodec(args[1])
        .create(path.getFileSystem(conf).create(path)));
    for(String inFile: TaxiToOrc.sliceArray(args, 2)) {
      JsonReader reader = new JsonReader(new Path(inFile), conf, schema);
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
