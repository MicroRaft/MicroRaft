package io.microraft.model.log;

import io.microraft.model.groupop.RaftGroupOp;
import io.microraft.model.groupop.UpdateRaftGroupMembersOp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.stream.Collectors.toList;

// TODO(szymon): move? => op.BatchOperation, op.group.UpdateRaftGroupMembersOp?

/**
 * Marks {@link BatchOperation#operations} to be committed and applied
 * atomically and in <code>List</code> order.
 * <p>
 * Committing atomically means that one operation will be committed if and only
 * if all other operations in the batch are committed. The commit index of the
 * operation is then defined to be the commit index of the
 * <code>BatchOperation</code>. <br>
 * The atomic application of the batch is part of the contract with the state
 * machine at
 * {@link io.microraft.statemachine.StateMachine#runBatch(long, List)}. See the
 * method documentation for details.
 * <p>
 * If there are multiple {@link RaftGroupOp} instances in the batch, only the
 * last one will be executed. The return value of all other {@link RaftGroupOp}
 * instances will be <code>null</code>.
 * </p>
 */
public class BatchOperation {
    private static final Logger LOGGER = LoggerFactory.getLogger(BatchOperation.class);

    private final List<Object> operations;

    @Nullable
    private final UpdateRaftGroupMembersOp lastGroupOp;

    public BatchOperation(@Nonnull List<Object> operations) {
        if (operations.isEmpty())
            throw new IllegalArgumentException("A BatchOperation must contain at least one operation");

        operations.forEach(Objects::requireNonNull);

        if (LOGGER.isWarnEnabled()) {
            long numGroupOps = operations.stream().filter(RaftGroupOp.class::isInstance).count();

            if (numGroupOps > 1) {
                LOGGER.warn("BatchOperation contains multiple RaftGroupOps. Only the last one will be applied.");
            }
        }

        operations.stream().filter(RaftGroupOp.class::isInstance).forEach(op -> {
            if (!(op instanceof UpdateRaftGroupMembersOp)) {
                throw new IllegalArgumentException("RaftGroupOp must be an instance of UpdateRaftGroupMembersOp");
            }
        });

        lastGroupOp = (UpdateRaftGroupMembersOp) operations.stream().filter(UpdateRaftGroupMembersOp.class::isInstance)
                .reduce((first, second) -> second).orElse(null);

        this.operations = operations;
    }

    @Nonnull
    public List<Object> getOperations() {
        return operations;
    }

    @Nonnull
    public List<Object> getStateMachineOperations() {
        return getOperations().stream().filter(op -> !(op instanceof RaftGroupOp)).collect(toList());
    }

    @Nonnull
    public Optional<UpdateRaftGroupMembersOp> getGroupOpToApply() {
        return lastGroupOp != null ? Optional.of(lastGroupOp) : Optional.empty();
    }
}
