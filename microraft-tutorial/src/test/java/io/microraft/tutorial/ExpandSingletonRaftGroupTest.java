package io.microraft.tutorial;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

import java.util.List;
import java.util.concurrent.CompletionException;

import org.junit.Test;

import io.microraft.MembershipChangeMode;
import io.microraft.Ordered;
import io.microraft.QueryPolicy;
import io.microraft.RaftEndpoint;
import io.microraft.RaftNode;
import io.microraft.exception.IndeterminateStateException;
import io.microraft.statemachine.StateMachine;
import io.microraft.tutorial.atomicregister.OperableAtomicRegister;
import io.microraft.tutorial.atomicregister.SnapshotableAtomicRegister;

public class ExpandSingletonRaftGroupTest extends BaseLocalTest {

    @Override
    protected StateMachine createStateMachine() {
        return new SnapshotableAtomicRegister();
    }

    @Override
    protected List<RaftEndpoint> getInitialMembers() {
        return List.of(LocalRaftEndpoint.newEndpoint());
    }

    @Test
    public void when_singleNodeRaftGroupStarted_then_leaderIsElected() {
        RaftNode leader = waitUntilLeaderElected();

        assertThat(leader).isNotNull();
        assertThat(leader.getTerm().getLeaderEndpoint()).isEqualTo(leader.getLocalEndpoint());
    }

    @Test
    public void when_singletonRaftGroupAddsNewVotingMemberWithoutStartingRaftNode_then_leaderLosesQuorum() {
        RaftNode leader = waitUntilLeaderElected();

        // quorum is 1, hence the leader can replicate operations on its own.
        leader.replicate(OperableAtomicRegister.newSetOperation("val")).join();

        try {
            RaftEndpoint newEndpoint = LocalRaftEndpoint.newEndpoint();
            // we are adding a follower which is a voting member.
            // the leader will add the follower to the effective member list,
            // and the quorum will be 2, since the majority of 2 nodes is 2.
            // since the second node is not alive, the leader will not be able
            // to commit this operation and lose its leadership after
            // the leader heartbeat timeout.
            leader.changeMembership(newEndpoint, MembershipChangeMode.ADD_OR_PROMOTE_TO_FOLLOWER, 0).join();
            fail();
        } catch (CompletionException e) {
            assertThat(e).hasCauseInstanceOf(IndeterminateStateException.class);
        }
    }

    @Test
    public void when_singletonRaftGroupAddsNewVotingMemberWithStartingRaftNode_then_leaderContinuesOperation() {
        RaftNode leader = waitUntilLeaderElected();

        RaftEndpoint newEndpoint = LocalRaftEndpoint.newEndpoint();
        RaftNode newNode = createRaftNode(newEndpoint);
        newNode.start();

        // with this call, the leader's effective member list will be updated
        // and the new quorum will be 2. since the new Raft node is also
        // started, the leader will be able to commit the operation.
        leader.changeMembership(newEndpoint, MembershipChangeMode.ADD_OR_PROMOTE_TO_FOLLOWER, 0).join();

        String value1 = "value1";
        leader.replicate(OperableAtomicRegister.newSetOperation(value1)).join();

        eventually(() -> {
            assertThat(query(newNode).getResult()).isEqualTo(value1);
        });
    }

    private Ordered<String> query(RaftNode raftNode) {
        return raftNode.<String>query(OperableAtomicRegister.newGetOperation(), QueryPolicy.EVENTUAL_CONSISTENCY, 0)
                .join();
    }

}
