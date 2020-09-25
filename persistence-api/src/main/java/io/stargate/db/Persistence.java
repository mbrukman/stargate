/*
 * Copyright The Stargate Authors
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
package io.stargate.db;

import com.datastax.oss.driver.shaded.guava.common.util.concurrent.Uninterruptibles;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.PersistenceBackedDataStore;
import io.stargate.db.schema.Schema;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import org.apache.cassandra.stargate.locator.InetAddressAndPort;

/**
 * A persistence layer that can be queried.
 *
 * <p>This is the interface that stargate API extensions uses (either directly, or through the
 * higher level {@link DataStore} API wraps a instance of this interface) to query the underlying
 * store, and thus the one interface that persistence extensions must implement.
 *
 * @param <T> the type of the config for the persistence implementation.
 * @param <C> the type of the "client state" class used by the persistence implementation.
 */
public interface Persistence<T, C> {

  /** Name describing the persistence implementation. */
  String name();

  void initialize(T config);

  void destroy();

  /**
   * Returns the current schema.
   *
   * @return The current schema.
   */
  Schema schema();

  void registerEventListener(EventListener listener);

  boolean isRpcReady(InetAddressAndPort endpoint);

  InetAddressAndPort getNativeAddress(InetAddressAndPort endpoint);

  ClientState<C> newClientState(SocketAddress remoteAddress, InetSocketAddress publicAddress);

  ClientState newClientState(String name);

  AuthenticatedUser<?> newAuthenticatedUser(String name);

  Authenticator getAuthenticator();

  default DataStore newDataStore(@Nonnull Parameters queryParameters) {
    Objects.requireNonNull(queryParameters);
    return new PersistenceBackedDataStore<>(this, queryParameters);
  }

  /**
   * The object that should be used to act as an 'unset' value for this persistence (for use in
   * {@link Statement#values()}).
   *
   * <p>Please note that persistence implementations are allowed to use <b>reference equality</b> to
   * detect this value, so the object returned by this method should be used "as-is" and should
   * <b>not</b> be copied (through {@link ByteBuffer#duplicate()} or any other method).
   */
  ByteBuffer unsetValue();

  CompletableFuture<? extends Result> execute(
      Statement statement, Parameters parameters, long queryStartNanoTime);

  CompletableFuture<? extends Result> prepare(String cql, Parameters parameters);

  CompletableFuture<? extends Result> batch(
      Batch batch, Parameters parameters, long queryStartNanoTime);

  boolean isInSchemaAgreement();

  /** Wait for schema to agree across the cluster */
  default void waitForSchemaAgreement() {
    for (int count = 0; count < 100; count++) {
      if (isInSchemaAgreement()) {
        return;
      }
      Uninterruptibles.sleepUninterruptibly(200, TimeUnit.MILLISECONDS);
    }
    throw new IllegalStateException("Failed to reach schema agreement after 20 seconds.");
  }

  void captureClientWarnings();

  List<String> getClientWarnings();

  void resetClientWarnings();
}
