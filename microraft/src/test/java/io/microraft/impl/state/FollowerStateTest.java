package io.microraft.impl.state;

import org.junit.Test;

import static io.microraft.impl.state.FollowerState.MAX_BACKOFF_ROUND;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author metanet
 */
public class FollowerStateTest {

    private final FollowerState followerState = new FollowerState(0, 1);

    @Test
    public void testFirstBackoffRoundWithoutExtra() {
        long flowControlSeqNo = followerState.setRequestBackoff(false);

        assertThat(flowControlSeqNo).isGreaterThan(0);
        assertThat(followerState.flowControlSeqNo()).isEqualTo(flowControlSeqNo);
        assertThat(followerState.backoffRound()).isEqualTo(1);
    }

    @Test
    public void testValidResponseReceivedOnFirstBackoffRoundWithoutExtra() {
        long flowControlSeqNo = followerState.setRequestBackoff(false);
        boolean success = followerState.responseReceived(flowControlSeqNo);

        assertThat(success).isTrue();
        assertThat(followerState.flowControlSeqNo()).isEqualTo(flowControlSeqNo);
        assertThat(followerState.backoffRound()).isEqualTo(0);
    }

    @Test
    public void testInvalidResponseReceivedOnFirstBackoffRoundWithoutExtra() {
        long flowControlSeqNo = followerState.setRequestBackoff(false);
        boolean success = followerState.responseReceived(flowControlSeqNo + 1);

        assertThat(success).isFalse();
        assertThat(followerState.flowControlSeqNo()).isEqualTo(flowControlSeqNo);
        assertThat(followerState.backoffRound()).isEqualTo(1);
    }

    @Test
    public void testCompletedFirstBackoffRoundWithoutExtra() {
        long flowControlSeqNo = followerState.setRequestBackoff(false);

        boolean backoffCompleted = followerState.completeBackoffRound();

        assertThat(backoffCompleted).isTrue();
        assertThat(followerState.flowControlSeqNo()).isEqualTo(flowControlSeqNo);
        assertThat(followerState.backoffRound()).isEqualTo(0);
    }

    @Test
    public void testFirstResponseReceivedAfterBackoffIsSetForSecondRequest() {
        long flowControlSeqNo1 = followerState.setRequestBackoff(false);
        followerState.completeBackoffRound();

        long flowControlSeqNo2 = followerState.setRequestBackoff(false);
        assertThat(flowControlSeqNo2).isGreaterThan(flowControlSeqNo1);

        boolean success = followerState.responseReceived(flowControlSeqNo1);

        assertThat(success).isFalse();
        assertThat(followerState.flowControlSeqNo()).isEqualTo(flowControlSeqNo2);
        assertThat(followerState.backoffRound()).isEqualTo(2);
    }

    @Test
    public void testFirstBackoffRoundWithExtra() {
        long flowControlSeqNo = followerState.setRequestBackoff(true);

        assertThat(flowControlSeqNo).isGreaterThan(0);
        assertThat(followerState.flowControlSeqNo()).isEqualTo(flowControlSeqNo);
        assertThat(followerState.backoffRound()).isEqualTo(2);
    }

    @Test
    public void testValidResponseReceivedOnFirstBackoffRoundWithExtra() {
        long flowControlSeqNo = followerState.setRequestBackoff(true);
        boolean success = followerState.responseReceived(flowControlSeqNo);

        assertThat(success).isTrue();
        assertThat(followerState.flowControlSeqNo()).isEqualTo(flowControlSeqNo);
        assertThat(followerState.backoffRound()).isEqualTo(0);
    }

    @Test
    public void testInvalidResponseReceivedOnFirstBackoffRoundWithExtra() {
        long flowControlSeqNo = followerState.setRequestBackoff(true);
        boolean success = followerState.responseReceived(flowControlSeqNo + 1);

        assertThat(success).isFalse();
        assertThat(followerState.flowControlSeqNo()).isEqualTo(flowControlSeqNo);
        assertThat(followerState.backoffRound()).isEqualTo(2);
    }

    @Test
    public void testCompletedFirstBackoffRoundWithExtra() {
        long flowControlSeqNo = followerState.setRequestBackoff(true);

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
    public void testFirstResponseReceivedAfterBackoffIsSetWithExtraForSecondRequest() {
        long flowControlSeqNo1 = followerState.setRequestBackoff(false);
        followerState.completeBackoffRound();

        long flowControlSeqNo2 = followerState.setRequestBackoff(true);
        assertThat(flowControlSeqNo2).isGreaterThan(flowControlSeqNo1);

        boolean success = followerState.responseReceived(flowControlSeqNo1);

        assertThat(success).isFalse();
        assertThat(followerState.flowControlSeqNo()).isEqualTo(flowControlSeqNo2);
        assertThat(followerState.backoffRound()).isEqualTo(3);
    }

    @Test
    public void testMaxBackoff() {
        long flowControlSeqNo = followerState.setMaxRequestBackoff();

        assertThat(flowControlSeqNo).isGreaterThan(0);
        assertThat(followerState.flowControlSeqNo()).isEqualTo(flowControlSeqNo);
        assertThat(followerState.backoffRound()).isEqualTo(MAX_BACKOFF_ROUND);
    }

    @Test
    public void testValidResponseReceivedAfterMaxBackoff() {
        long flowControlSeqNo = followerState.setMaxRequestBackoff();
        boolean success = followerState.responseReceived(flowControlSeqNo);

        assertThat(success).isTrue();
        assertThat(followerState.flowControlSeqNo()).isEqualTo(flowControlSeqNo);
        assertThat(followerState.backoffRound()).isEqualTo(0);
    }

    @Test
    public void testInvalidResponseReceivedAfterMaxBackoff() {
        long flowControlSeqNo = followerState.setMaxRequestBackoff();
        boolean success = followerState.responseReceived(flowControlSeqNo + 1);

        assertThat(success).isFalse();
        assertThat(followerState.flowControlSeqNo()).isEqualTo(flowControlSeqNo);
        assertThat(followerState.backoffRound()).isEqualTo(MAX_BACKOFF_ROUND);
    }

    @Test
    public void testPassOverAllBackoffRoundsWhenMaxBackoffIsSet() {
        followerState.setMaxRequestBackoff();
        int count = 0;

        do {
            count++;
            assertThat(followerState.backoffRound()).isGreaterThan(0);
        } while (!followerState.completeBackoffRound());

        assertThat(followerState.backoffRound()).isEqualTo(0);
        assertThat(count).isEqualTo(MAX_BACKOFF_ROUND);
    }

    @Test
    public void testFirstResponseReceivedAfterMaxBackoffIsSetForSecondRequest() {
        long flowControlSeqNo1 = followerState.setRequestBackoff(false);
        followerState.completeBackoffRound();

        long flowControlSeqNo2 = followerState.setMaxRequestBackoff();
        boolean success = followerState.responseReceived(flowControlSeqNo1);

        assertThat(success).isFalse();
        assertThat(followerState.flowControlSeqNo()).isEqualTo(flowControlSeqNo2);
        assertThat(followerState.backoffRound()).isEqualTo(MAX_BACKOFF_ROUND);
    }

}
