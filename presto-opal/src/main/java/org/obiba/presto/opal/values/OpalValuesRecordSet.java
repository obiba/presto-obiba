/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.obiba.presto.opal.values;

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.obiba.presto.RestColumnHandle;
import org.obiba.presto.RestRecordSet;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class OpalValuesRecordSet extends RestRecordSet {
  private final OpalValuesRest rest;
  private final SchemaTableName schemaTableName;

  protected OpalValuesRecordSet(OpalValuesRest rest, SchemaTableName schemaTableName, List<RestColumnHandle> restColumnHandles) {
    super(restColumnHandles);
    this.rest = rest;
    this.schemaTableName =schemaTableName;
  }

  @Override
  public RecordCursor cursor() {
    return new ValueSetCursor();
  }

  private class ValueSetCursor implements RecordCursor {
    private int offset = 0;
    private Iterator<? extends List<?>> records;
    private List<?> record;
    private boolean closed;

    @Override
    public long getTotalBytes() {
      return 0;
    }

    @Override
    public long getCompletedBytes() {
      return 0;
    }

    @Override
    public long getReadTimeNanos() {
      return 0;
    }

    @Override
    public Type getType(int field) {
      return getColumnTypes().get(field);
    }

    @Override
    public boolean advanceNextPosition() {
      if (closed) return false;
      record = null;
      if(records == null || !records.hasNext()) {
        records = rest.getRows(schemaTableName, restColumnHandles, offset).iterator();
        offset += OpalValuesRest.BATCH_SIZE;
        closed = !records.hasNext();
      }
      if (records.hasNext()) record = records.next();
      closed = record == null;
      return record != null;
    }

    @Override
    public boolean getBoolean(int field) {
      checkState(record != null, "no current record");
      checkNotNull(record.get(field), "value is null");
      return (Boolean) record.get(field);
    }

    @Override
    public long getLong(int field) {
      checkState(record != null, "no current record");
      checkNotNull(record.get(field), "value is null");
      return ((Number) record.get(field)).longValue();
    }

    @Override
    public double getDouble(int field) {
      checkState(record != null, "no current record");
      checkNotNull(record.get(field), "value is null");
      return (Double) record.get(field);
    }

    @Override
    public Slice getSlice(int field) {
      checkState(record != null, "no current record");
      Object value = record.get(field);
      checkNotNull(value, "value is null");
      if (value instanceof byte[]) {
        return Slices.wrappedBuffer((byte[]) value);
      }
      if (value instanceof String) {
        return Slices.utf8Slice((String) value);
      }
      if (value instanceof Slice) {
        return (Slice) value;
      }
      throw new IllegalArgumentException("Field " + field + " is not a String, but is a " + value.getClass().getName());
    }

    @Override
    public Object getObject(int field) {
      checkState(record != null, "no current record");
      Object value = record.get(field);
      checkNotNull(value, "value is null");
      return value;
    }

    @Override
    public boolean isNull(int field) {
      checkState(record != null, "no current record");
      return record.get(field) == null;
    }

    @Override
    public void close() {
      closed = true;
    }
  }

  private static void checkState(boolean test, String message) {
    if (!test) throw new IllegalStateException(message);
  }

  private static void checkNotNull(Object value, String message) {
    if (value == null) {
      throw new NullPointerException(message);
    }
  }
}
