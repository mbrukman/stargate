package io.stargate.db.datastore;

import io.stargate.db.Parameters;
import io.stargate.db.Persistence;
import io.stargate.db.Result;
import io.stargate.db.schema.Index;
import io.stargate.db.schema.Schema;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.cassandra.stargate.db.ConsistencyLevel;

public class PersistenceBackedDataStore<T, C> implements DataStore {

  private final Persistence<T, C> persistence;
  private final Parameters parameters;

  public PersistenceBackedDataStore(Persistence<T, C> persistence, Parameters parameters) {
    this.persistence = persistence;
    this.parameters = parameters;
  }

  @Override
  public CompletableFuture<ResultSet> query(
      String cql, Optional<ConsistencyLevel> consistencyLevel, Object... values) {
    return prepare(cql, Optional.empty()).thenCompose(p -> p.execute(consistencyLevel, values));
  }

  @Override
  public CompletableFuture<PreparedStatement> prepare(String cql, Optional<Index> index) {
    return persistence
        .prepare(cql, parameters)
        .thenApply(
            r -> {
              assert r instanceof Result.Prepared;
              Result.Prepared prepared = (Result.Prepared) r;
              return new PersistenceBackedPreparedStatement(
                  persistence, parameters, prepared.statementId, prepared.metadata.columns);
            });
  }

  @Override
  public Schema schema() {
    return persistence.schema();
  }

  @Override
  public boolean isInSchemaAgreement() {
    return persistence.isInSchemaAgreement();
  }

  @Override
  public void waitForSchemaAgreement() {
    persistence.waitForSchemaAgreement();
  }
}
