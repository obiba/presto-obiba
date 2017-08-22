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

package org.obiba.presto;

import com.facebook.presto.spi.*;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;

import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.function.Consumer;

import static java.util.stream.Collectors.toList;

public interface Rest
{
    ConnectorTableMetadata getTableMetadata(SchemaTableName schemaTableName);

    List<String> listSchemas();

    default List<SchemaTableName> listTables()
    {
        return listSchemas().stream()
                .flatMap(schema -> listTables(schema).stream())
                .collect(toList());
    }

    List<SchemaTableName> listTables(String schema);

    Collection<? extends List<?>> getRows(SchemaTableName schemaTableName, List<RestColumnHandle> restColumnHandles, TupleDomain<ColumnHandle> tupleDomain);

    default RecordSet getRecordSet(SchemaTableName schemaTableName, List<RestColumnHandle> restColumnHandles, TupleDomain<ColumnHandle> tupleDomain)
    {
        Collection<? extends List<?>> rows = getRows(schemaTableName, restColumnHandles, tupleDomain);
        List<Type> mappedTypes = restColumnHandles.stream()
            .map(RestColumnHandle::getType)
            .collect(toList());
        return new InMemoryRecordSet(mappedTypes, rows);
    }

    default List<Type> getTypes(SchemaTableName schemaTableName)
    {
        return getTableMetadata(schemaTableName).getColumns().stream()
                .map(ColumnMetadata::getType)
                .collect(toList());
    }

    default String normalize(String name) {
        return name.toLowerCase(Locale.ENGLISH).replace(' ', '_').replace('-', '_').replace("(", "").replace(")", "");
    }

    default boolean supportsPaging() { return false; }
}
