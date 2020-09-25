/*
 * Copyright DataStax, Inc. and/or The Stargate Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.db.datastore;

import io.stargate.db.datastore.query.QueryBuilder;
import io.stargate.db.schema.Index;
import io.stargate.db.schema.Schema;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.cassandra.stargate.db.ConsistencyLevel;

/**
 * This will be our interface in to the rest of DSE. By using this rather than calling static
 * methods we have a fighting chance of being able to unit test without starting C*.
 */
public interface DataStore {
  /** The fetch size for SELECT statements */
  int DEFAULT_ROWS_PER_PAGE = 1000;

  /** Create a query using the DSL builder. */
  default QueryBuilder query() {
    return new QueryBuilder(this);
  }

  default CompletableFuture<ResultSet> query(String cql, Object... parameters) {
    return query(cql, Optional.empty(), parameters);
  }

  CompletableFuture<ResultSet> query(
      String cql, Optional<ConsistencyLevel> consistencyLevel, Object... parameters);

  default CompletableFuture<PreparedStatement> prepare(String cql) {
    return prepare(cql, Optional.empty());
  }

  CompletableFuture<PreparedStatement> prepare(String cql, Optional<Index> index);

  /**
   * Returns the current schema.
   *
   * @return The current schema.
   */
  Schema schema();

  /** Returns true if in schema agreement */
  boolean isInSchemaAgreement();

  /** Wait for schema to agree across the cluster */
  void waitForSchemaAgreement();
}
