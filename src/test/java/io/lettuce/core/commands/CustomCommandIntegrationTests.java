/*
 * Copyright 2011-Present, Redis Ltd. and Contributors
 * All rights reserved.
 *
 * Licensed under the MIT License.
 *
 * This file contains contributions from third-party contributors
 * licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.lettuce.core.commands;

import static io.lettuce.TestTags.INTEGRATION_TEST;
import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.*;

import java.util.Arrays;

import javax.inject.Inject;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.TestSupport;
import io.lettuce.core.TransactionResult;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.output.StatusOutput;
import io.lettuce.core.output.VoidOutput;
import io.lettuce.core.protocol.AsyncCommand;
import io.lettuce.core.protocol.Command;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.CommandType;
import io.lettuce.core.protocol.ProtocolKeyword;
import io.lettuce.core.protocol.RedisCommand;
import io.lettuce.test.LettuceExtension;
import io.lettuce.test.TestFutures;

/**
 * Integration tests for {@link RedisCommands#dispatch}.
 *
 * @author Mark Paluch
 */
@Tag(INTEGRATION_TEST)
@ExtendWith(LettuceExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class CustomCommandIntegrationTests extends TestSupport {

    private final RedisCommands<String, String> redis;

    @Inject
    CustomCommandIntegrationTests(StatefulRedisConnection<String, String> connection) {
        this.redis = connection.sync();
    }

    @BeforeEach
    void setUp() {
        this.redis.flushall();
    }

    @Test
    void dispatchSet() {

        String response = redis.dispatch(MyCommands.SET, new StatusOutput<>(StringCodec.UTF8),
                new CommandArgs<>(StringCodec.UTF8).addKey(key).addValue(value));

        assertThat(response).isEqualTo("OK");
    }

    @Test
    void dispatchWithoutArgs() {

        String response = redis.dispatch(MyCommands.INFO, new StatusOutput<>(StringCodec.UTF8));

        assertThat(response).contains("connected_clients");
    }

    @Test
    void dispatchFireAndForget() {
        redis.set(key, value);
        redis.dispatch(CommandType.LLEN, VoidOutput.create(), new CommandArgs<>(StringCodec.UTF8).addKey(key));
    }

    @Test
    void dispatchNoOutputButError() {

        redis.set(key, value);

        assertThatExceptionOfType(RedisCommandExecutionException.class).isThrownBy(
                () -> redis.dispatch(CommandType.LLEN, new VoidOutput<>(), new CommandArgs<>(StringCodec.UTF8).addKey(key)))
                .withMessageStartingWith("WRONGTYPE");
    }

    @Test
    void dispatchShouldFailForWrongDataType() {

        redis.hset(key, key, value);
        assertThatThrownBy(() -> redis.dispatch(CommandType.GET, new StatusOutput<>(StringCodec.UTF8),
                new CommandArgs<>(StringCodec.UTF8).addKey(key))).isInstanceOf(RedisCommandExecutionException.class);
    }

    @Test
    void dispatchTransactions() {

        redis.multi();
        String response = redis.dispatch(CommandType.SET, new StatusOutput<>(StringCodec.UTF8),
                new CommandArgs<>(StringCodec.UTF8).addKey(key).addValue(value));

        TransactionResult exec = redis.exec();

        assertThat(response).isNull();
        assertThat(exec).hasSize(1).contains("OK");
    }

    @Test
    void dispatchMulti() {
        String response = redis.dispatch(CommandType.MULTI, new StatusOutput<>(StringCodec.UTF8));
        assertThat(response).isEqualTo("OK");
        TransactionResult exec = redis.exec();
        assertThat(exec).isEmpty();
    }

    @Test
    void standaloneAsyncPing() {

        RedisCommand<String, String, String> command = new Command<>(MyCommands.PING, new StatusOutput<>(StringCodec.UTF8),
                null);

        AsyncCommand<String, String, String> async = new AsyncCommand<>(command);
        getStandaloneConnection().dispatch(async);

        assertThat(TestFutures.getOrTimeout(async.toCompletableFuture())).isEqualTo("PONG");
    }

    @Test
    void standaloneAsyncBatchPing() {

        RedisCommand<String, String, String> command1 = new Command<>(MyCommands.PING, new StatusOutput<>(StringCodec.UTF8),
                null);

        RedisCommand<String, String, String> command2 = new Command<>(MyCommands.PING, new StatusOutput<>(StringCodec.UTF8),
                null);

        AsyncCommand<String, String, String> async1 = new AsyncCommand<>(command1);
        AsyncCommand<String, String, String> async2 = new AsyncCommand<>(command2);
        getStandaloneConnection().dispatch(Arrays.asList(async1, async2));

        assertThat(TestFutures.getOrTimeout(async1.toCompletableFuture())).isEqualTo("PONG");
        assertThat(TestFutures.getOrTimeout(async2.toCompletableFuture())).isEqualTo("PONG");
    }

    @Test
    void standaloneAsyncBatchTransaction() {

        RedisCommand<String, String, String> multi = new Command<>(CommandType.MULTI, new StatusOutput<>(StringCodec.UTF8));

        RedisCommand<String, String, String> set = new Command<>(CommandType.SET, new StatusOutput<>(StringCodec.UTF8),
                new CommandArgs<>(StringCodec.UTF8).addKey("key").add("value"));

        RedisCommand<String, String, TransactionResult> exec = new Command<>(CommandType.EXEC, null);

        AsyncCommand<String, String, String> async1 = new AsyncCommand<>(multi);
        AsyncCommand<String, String, String> async2 = new AsyncCommand<>(set);
        AsyncCommand<String, String, TransactionResult> async3 = new AsyncCommand<>(exec);
        getStandaloneConnection().dispatch(Arrays.asList(async1, async2, async3));

        assertThat(TestFutures.getOrTimeout(async1.toCompletableFuture())).isEqualTo("OK");
        assertThat(TestFutures.getOrTimeout(async2.toCompletableFuture())).isEqualTo("OK");

        TransactionResult transactionResult = TestFutures.getOrTimeout(async3.toCompletableFuture());
        assertThat(transactionResult.wasDiscarded()).isFalse();
        assertThat(transactionResult.<String> get(0)).isEqualTo("OK");
    }

    @Test
    void standaloneFireAndForget() {

        RedisCommand<String, String, String> command = new Command<>(MyCommands.PING, new StatusOutput<>(StringCodec.UTF8),
                null);
        getStandaloneConnection().dispatch(command);
        assertThat(command.isCancelled()).isFalse();

    }

    private StatefulRedisConnection<String, String> getStandaloneConnection() {

        assumeTrue(redis.getStatefulConnection() instanceof StatefulRedisConnection);
        return redis.getStatefulConnection();
    }

    public enum MyCommands implements ProtocolKeyword {

        PING, SET, INFO;

        private final byte name[];

        MyCommands() {
            // cache the bytes for the command name. Reduces memory and cpu pressure when using commands.
            name = name().getBytes();
        }

        @Override
        public byte[] getBytes() {
            return name;
        }

    }

}
