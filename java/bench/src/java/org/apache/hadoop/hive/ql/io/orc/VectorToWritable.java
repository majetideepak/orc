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

package org.apache.hadoop.hive.ql.io.orc;

import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.orc.OrcProto;
import org.apache.orc.OrcUtils;
import org.apache.orc.TypeDescription;

import java.util.List;

/**
 * This class is just here to provide a public API to some of the ORC internal
 * methods.
 */
public class VectorToWritable {
  public static ObjectInspector createObjectInspector(TypeDescription schema) {
    // convert the type descr to protobuf types
    List<OrcProto.Type> types = OrcUtils.getOrcTypes(schema);
    // convert the protobuf types to an ObjectInspector
    return OrcStruct.createObjectInspector(0, types);
  }

  public static Object createValue(VectorizedRowBatch batch,
                                   int row,
                                   TypeDescription schema,
                                   Object previous) {
    if(schema.getCategory() == TypeDescription.Category.STRUCT) {
      List<TypeDescription> children = schema.getChildren();
      int numberOfChildren = children.size();
      OrcStruct result;
      if(previous != null && previous.getClass() == OrcStruct.class) {
        result = (OrcStruct)previous;
        if(result.getNumFields() != numberOfChildren) {
          result.setNumFields(numberOfChildren);
        }
      } else {
        result = new OrcStruct(numberOfChildren);
        previous = result;
      }

      for(int i = 0; i < numberOfChildren; ++i) {
        result.setFieldValue(i, RecordReaderImpl.nextValue(batch.cols[i], row,
            children.get(i), result.getFieldValue(i)));
      }
    } else {
      previous = RecordReaderImpl.nextValue(batch.cols[0], row, schema,
          previous);
    }
    ;
    return previous;
  }
}
