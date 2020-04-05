/*
 * Copyright (c) 2020, MicroRaft.
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

package io.microraft.examples;

import io.microraft.Ordered;
import io.microraft.RaftNode;
import io.microraft.impl.local.LocalRaftGroup;
import io.microraft.impl.local.SimpleStateMachine;
import io.microraft.impl.util.BaseTest;
import org.junit.After;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/*

   TO RUN THIS CODE SAMPLE ON YOUR MACHINE:

 $ git clone git@github.com:metanet/MicroRaft.git
 $ cd MicroRaft && ./mvnw clean test -Dtest=io.microraft.examples.OperationCommitTest -DfailIfNoTests=false -Pcode-sample

 */
public class OperationCommitTest
        extends BaseTest {

    private LocalRaftGroup group;

    @After
    public void tearDown() {
        if (group != null) {
            group.destroy();
        }
    }

    @Test
    public void testCommitOperation() {
        group = LocalRaftGroup.start(3);
        RaftNode leader = group.waitUntilLeaderElected();

        String value = "value";
        Object operation = SimpleStateMachine.apply(value);
        CompletableFuture<Ordered<String>> future = leader.replicate(operation);
        Ordered<String> ordered = future.join();

        System.out.println("operation result: " + ordered.getResult() + ", commit index: " + ordered.getCommitIndex());

        assertThat(ordered.getResult()).isEqualTo(value);
        assertThat(ordered.getCommitIndex()).isEqualTo(1);
    }

}
