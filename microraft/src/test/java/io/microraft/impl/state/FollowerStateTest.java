package io.microraft.impl.state;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class FollowerStateTest {
    private static final long TIME = 12345;

    private final FollowerState followerState = new FollowerState(0, 1, TIME);

    @Test
    public void testFirstBackoffRound() {
        long flowControlSeqNum = followerState.setRequestBackoff(1, 2);

        assertThat(flowControlSeqNum).isGreaterThan(0);
        assertThat(followerState.flowControlSequenceNumber()).isEqualTo(flowControlSeqNum);
        assertThat(followerState.backoffRound()).isEqualTo(1);
    }

    @Test
    public void testValidResponseReceivedOnFirstBackoff() {
        long flowControlSeqNum = followerState.setRequestBackoff(1, 2);
        boolean success = followerState.responseReceived(flowControlSeqNum, TIME);

        assertThat(success).isTrue();
        assertThat(followerState.flowControlSequenceNumber()).isEqualTo(flowControlSeqNum);
        assertThat(followerState.backoffRound()).isEqualTo(0);
    }

    @Test
    public void testInvalidResponseReceivedOnFirstBackoff() {
        long flowControlSeqNum = followerState.setRequestBackoff(1, 2);
        boolean success = followerState.responseReceived(flowControlSeqNum + 1, TIME);

        assertThat(success).isFalse();
        assertThat(followerState.flowControlSequenceNumber()).isEqualTo(flowControlSeqNum);
        assertThat(followerState.backoffRound()).isEqualTo(1);
    }

    @Test
    public void testCompletedFirstBackoff() {
        long flowControlSeqNum = followerState.setRequestBackoff(1, 2);

        boolean backoffCompleted = followerState.completeBackoffRound();

        assertThat(backoffCompleted).isTrue();
        assertThat(followerState.flowControlSequenceNumber()).isEqualTo(flowControlSeqNum);
        assertThat(followerState.backoffRound()).isEqualTo(0);
    }

    @Test
    public void testFirstResponseReceivedAfterBackoffIsSetForSecondRequest() {
        long flowControlSeqNum1 = followerState.setRequestBackoff(1, 2);
        followerState.completeBackoffRound();

        long flowControlSeqNum2 = followerState.setRequestBackoff(1, 2);
        assertThat(flowControlSeqNum2).isGreaterThan(flowControlSeqNum1);

        boolean success = followerState.responseReceived(flowControlSeqNum1, TIME);

        assertThat(success).isFalse();
        assertThat(followerState.flowControlSequenceNumber()).isEqualTo(flowControlSeqNum2);
        assertThat(followerState.backoffRound()).isEqualTo(2);
    }

    @Test
    public void testSecondBackoffRound() {
        followerState.setRequestBackoff(1, 2);
        followerState.completeBackoffRound();

        long flowControlSeqNum = followerState.setRequestBackoff(1, 2);

        assertThat(flowControlSeqNum).isGreaterThan(0);
        assertThat(followerState.flowControlSequenceNumber()).isEqualTo(flowControlSeqNum);
        assertThat(followerState.backoffRound()).isEqualTo(2);
    }

    @Test
    public void testValidResponseReceivedOnSecondBackoff() {
        followerState.setRequestBackoff(1, 2);
        followerState.completeBackoffRound();

        long flowControlSeqNum = followerState.setRequestBackoff(1, 2);
        boolean success = followerState.responseReceived(flowControlSeqNum, TIME);

        assertThat(success).isTrue();
        assertThat(followerState.flowControlSequenceNumber()).isEqualTo(flowControlSeqNum);
        assertThat(followerState.backoffRound()).isEqualTo(0);
    }

    @Test
    public void testInvalidResponseReceivedOnSecondBackoff() {
        followerState.setRequestBackoff(1, 2);
        followerState.completeBackoffRound();

        long flowControlSeqNum = followerState.setRequestBackoff(1, 2);
        boolean success = followerState.responseReceived(flowControlSeqNum + 1, TIME);

        assertThat(success).isFalse();
        assertThat(followerState.flowControlSequenceNumber()).isEqualTo(flowControlSeqNum);
        assertThat(followerState.backoffRound()).isEqualTo(2);
    }

    @Test
    public void testCompletedSecondBackoff() {
        followerState.setRequestBackoff(1, 2);
        followerState.completeBackoffRound();

        long flowControlSeqNum = followerState.setRequestBackoff(1, 2);

        boolean backoffCompleted1 = followerState.completeBackoffRound();

        assertThat(backoffCompleted1).isFalse();
        assertThat(followerState.flowControlSequenceNumber()).isEqualTo(flowControlSeqNum);
        assertThat(followerState.backoffRound()).isEqualTo(1);

        boolean backoffCompleted2 = followerState.completeBackoffRound();

        assertThat(backoffCompleted2).isTrue();
        assertThat(followerState.flowControlSequenceNumber()).isEqualTo(flowControlSeqNum);
        assertThat(followerState.backoffRound()).isEqualTo(0);
    }

    @Test
    public void testFirstResponseReceivedAfterSecondBackoffIsSetForSecondRequest() {
        long flowControlSeqNum1 = followerState.setRequestBackoff(1, 2);
        followerState.completeBackoffRound();

        long flowControlSeqNum2 = followerState.setRequestBackoff(1, 2);
        assertThat(flowControlSeqNum2).isGreaterThan(flowControlSeqNum1);

        boolean success = followerState.responseReceived(flowControlSeqNum1, TIME);

        assertThat(success).isFalse();
        assertThat(followerState.flowControlSequenceNumber()).isEqualTo(flowControlSeqNum2);
        assertThat(followerState.backoffRound()).isEqualTo(2);
    }

    @Test
    public void testMaxBackoff() {
        long flowControlSeqNum1 = followerState.setRequestBackoff(1, 4);
        followerState.completeBackoffRound();

        long flowControlSeqNum2 = followerState.setRequestBackoff(1, 4);
        followerState.completeBackoffRound();
        followerState.completeBackoffRound();

        long flowControlSeqNum3 = followerState.setRequestBackoff(1, 4);
        followerState.completeBackoffRound();
        followerState.completeBackoffRound();
        followerState.completeBackoffRound();
        followerState.completeBackoffRound();

        long flowControlSeqNum4 = followerState.setRequestBackoff(1, 4);

        assertThat(flowControlSeqNum4).isGreaterThan(flowControlSeqNum3);
        assertThat(flowControlSeqNum3).isGreaterThan(flowControlSeqNum2);
        assertThat(flowControlSeqNum2).isGreaterThan(flowControlSeqNum1);
        assertThat(followerState.flowControlSequenceNumber()).isEqualTo(flowControlSeqNum4);
        assertThat(followerState.backoffRound()).isEqualTo(4);
    }

}
