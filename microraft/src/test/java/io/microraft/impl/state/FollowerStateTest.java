package io.microraft.impl.state;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class FollowerStateTest {

    private final FollowerState followerState = new FollowerState(0, 1);

    @Test
    public void testFirstBackoffRound() {
        long flowControlSeqNo = followerState.setRequestBackoff(1, 2);

        assertThat(flowControlSeqNo).isGreaterThan(0);
        assertThat(followerState.flowControlSeqNo()).isEqualTo(flowControlSeqNo);
        assertThat(followerState.backoffRound()).isEqualTo(1);
    }

    @Test
    public void testValidResponseReceivedOnFirstBackoff() {
        long flowControlSeqNo = followerState.setRequestBackoff(1, 2);
        boolean success = followerState.responseReceived(flowControlSeqNo);

        assertThat(success).isTrue();
        assertThat(followerState.flowControlSeqNo()).isEqualTo(flowControlSeqNo);
        assertThat(followerState.backoffRound()).isEqualTo(0);
    }

    @Test
    public void testInvalidResponseReceivedOnFirstBackoff() {
        long flowControlSeqNo = followerState.setRequestBackoff(1, 2);
        boolean success = followerState.responseReceived(flowControlSeqNo + 1);

        assertThat(success).isFalse();
        assertThat(followerState.flowControlSeqNo()).isEqualTo(flowControlSeqNo);
        assertThat(followerState.backoffRound()).isEqualTo(1);
    }

    @Test
    public void testCompletedFirstBackoff() {
        long flowControlSeqNo = followerState.setRequestBackoff(1, 2);

        boolean backoffCompleted = followerState.completeBackoffRound();

        assertThat(backoffCompleted).isTrue();
        assertThat(followerState.flowControlSeqNo()).isEqualTo(flowControlSeqNo);
        assertThat(followerState.backoffRound()).isEqualTo(0);
    }

    @Test
    public void testFirstResponseReceivedAfterBackoffIsSetForSecondRequest() {
        long flowControlSeqNo1 = followerState.setRequestBackoff(1, 2);
        followerState.completeBackoffRound();

        long flowControlSeqNo2 = followerState.setRequestBackoff(1, 2);
        assertThat(flowControlSeqNo2).isGreaterThan(flowControlSeqNo1);

        boolean success = followerState.responseReceived(flowControlSeqNo1);

        assertThat(success).isFalse();
        assertThat(followerState.flowControlSeqNo()).isEqualTo(flowControlSeqNo2);
        assertThat(followerState.backoffRound()).isEqualTo(2);
    }

    @Test
    public void testSecondBackoffRound() {
        followerState.setRequestBackoff(1, 2);
        followerState.completeBackoffRound();

        long flowControlSeqNo = followerState.setRequestBackoff(1, 2);

        assertThat(flowControlSeqNo).isGreaterThan(0);
        assertThat(followerState.flowControlSeqNo()).isEqualTo(flowControlSeqNo);
        assertThat(followerState.backoffRound()).isEqualTo(2);
    }

    @Test
    public void testValidResponseReceivedOnSecondBackoff() {
        followerState.setRequestBackoff(1, 2);
        followerState.completeBackoffRound();

        long flowControlSeqNo = followerState.setRequestBackoff(1, 2);
        boolean success = followerState.responseReceived(flowControlSeqNo);

        assertThat(success).isTrue();
        assertThat(followerState.flowControlSeqNo()).isEqualTo(flowControlSeqNo);
        assertThat(followerState.backoffRound()).isEqualTo(0);
    }

    @Test
    public void testInvalidResponseReceivedOnSecondBackoff() {
        followerState.setRequestBackoff(1, 2);
        followerState.completeBackoffRound();

        long flowControlSeqNo = followerState.setRequestBackoff(1, 2);
        boolean success = followerState.responseReceived(flowControlSeqNo + 1);

        assertThat(success).isFalse();
        assertThat(followerState.flowControlSeqNo()).isEqualTo(flowControlSeqNo);
        assertThat(followerState.backoffRound()).isEqualTo(2);
    }

    @Test
    public void testCompletedSecondBackoff() {
        followerState.setRequestBackoff(1, 2);
        followerState.completeBackoffRound();

        long flowControlSeqNo = followerState.setRequestBackoff(1, 2);

        boolean backoffCompleted1 = followerState.completeBackoffRound();

        assertThat(backoffCompleted1).isFalse();
        assertThat(followerState.flowControlSeqNo()).isEqualTo(flowControlSeqNo);
        assertThat(followerState.backoffRound()).isEqualTo(1);

        boolean backoffCompleted2 = followerState.completeBackoffRound();

        assertThat(backoffCompleted2).isTrue();
        assertThat(followerState.flowControlSeqNo()).isEqualTo(flowControlSeqNo);
        assertThat(followerState.backoffRound()).isEqualTo(0);
    }

    @Test
    public void testFirstResponseReceivedAfterSecondBackoffIsSetForSecondRequest() {
        long flowControlSeqNo1 = followerState.setRequestBackoff(1, 2);
        followerState.completeBackoffRound();

        long flowControlSeqNo2 = followerState.setRequestBackoff(1, 2);
        assertThat(flowControlSeqNo2).isGreaterThan(flowControlSeqNo1);

        boolean success = followerState.responseReceived(flowControlSeqNo1);

        assertThat(success).isFalse();
        assertThat(followerState.flowControlSeqNo()).isEqualTo(flowControlSeqNo2);
        assertThat(followerState.backoffRound()).isEqualTo(2);
    }

    @Test
    public void testMaxBackoff() {
        long flowControlSeqNo1 = followerState.setRequestBackoff(1, 4);
        followerState.completeBackoffRound();

        long flowControlSeqNo2 = followerState.setRequestBackoff(1, 4);
        followerState.completeBackoffRound();
        followerState.completeBackoffRound();

        long flowControlSeqNo3 = followerState.setRequestBackoff(1, 4);
        followerState.completeBackoffRound();
        followerState.completeBackoffRound();
        followerState.completeBackoffRound();
        followerState.completeBackoffRound();

        long flowControlSeqNo4 = followerState.setRequestBackoff(1, 4);

        assertThat(flowControlSeqNo4).isGreaterThan(flowControlSeqNo3);
        assertThat(flowControlSeqNo3).isGreaterThan(flowControlSeqNo2);
        assertThat(flowControlSeqNo2).isGreaterThan(flowControlSeqNo1);
        assertThat(followerState.flowControlSeqNo()).isEqualTo(flowControlSeqNo4);
        assertThat(followerState.backoffRound()).isEqualTo(4);
    }

}
