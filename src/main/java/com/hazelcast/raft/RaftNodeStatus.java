package com.hazelcast.raft;

import com.hazelcast.raft.exception.NotLeaderException;
import com.hazelcast.raft.exception.RaftException;
import com.hazelcast.raft.exception.RaftGroupTerminatedException;

/**
 * Statuses of a Raft node during its lifecycle.
 *
 * @author mdogan
 * @author metanet
 * @see RaftNode
 */
public enum RaftNodeStatus {

    /**
     * Initial state of a node. The Raft node stays in this state until it is
     * started via {@link RaftNode#start()}.
     */
    INITIAL {
        @Override
        public boolean isTerminal() {
            return false;
        }
    },

    /**
     * The Raft node remains in this state when there is no ongoing membership
     * change or a Raft group termination process. Operations are replicated
     * when the Raft node is in this state.
     */
    ACTIVE {
        @Override
        public boolean isTerminal() {
            return false;
        }
    },

    /**
     * When a membership change is appended to the Raft log, the Raft node
     * switches to this state and remains in this state until the membership
     * change is committed or reverted. After the membership change is
     * committed, if the Raft node is removed from the Raft group, the status
     * becomes {@link #STEPPED_DOWN}, otherwise it becomes {@link #ACTIVE}
     * again. New operations can be replicated while there is an ongoing
     * membership change in the Raft group, but no other membership change,
     * Raft group termination or leadership transfer can be triggered.
     */
    UPDATING_GROUP_MEMBER_LIST {
        @Override
        public boolean isTerminal() {
            return false;
        }
    },

    /**
     * When the Raft node is removed from the Raft group, its status becomes
     * {@code STEPPED_DOWN}.
     */
    STEPPED_DOWN {
        @Override
        public boolean isTerminal() {
            return true;
        }

        @Override
        public RaftException createException(RaftEndpoint localEndpoint) {
            return new NotLeaderException(localEndpoint, null);
        }
    },

    /**
     * When the Raft group termination operation is appended, the Raft node
     * switches to this state and remains in this state until the termination
     * is committed or reverted. If the termination process is committed, then
     * the status becomes {@link #TERMINATED}. Otherwise, it goes back to
     * the {@link #ACTIVE}. No new operations can be appended, no membership
     * change and leadership transfer can be triggered in this state.
     */
    TERMINATING {
        @Override
        public boolean isTerminal() {
            return false;
        }
    },

    /**
     * When a Raft group is terminated, status become {@code TERMINATED}.
     * A Raft node stops running the Raft consensus algorithm once it moves to
     * this state.
     */
    TERMINATED {
        @Override
        public boolean isTerminal() {
            return true;
        }

        @Override
        public RaftException createException(RaftEndpoint localEndpoint) {
            return new RaftGroupTerminatedException();
        }
    };

    /**
     * Returns if the Raft node status is a terminal. A Raft node stops running
     * the Raft consensus algorithm when it switches to a terminal status.
     */
    public abstract boolean isTerminal();

    /**
     * Returns the exception (or null) that a Raft node can throw in the
     * current status
     */
    public RaftException createException(RaftEndpoint localEndpoint) {
        return null;
    }

}
