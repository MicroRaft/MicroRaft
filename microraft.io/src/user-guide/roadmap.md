I am planning to work on the following tasks in future, but have no strict
plans. If you have ideas, just [chime in](https://join.slack.com/t/microraft/shared_invite/zt-dc6utpfk-84P0VbK7EcrD3lIme2IaaQ)! 

- Learner nodes. When a new Raft node is added to a running Raft group, it can
start with the "learner" role. In this role, the new Raft node is excluded in 
the quorum calculations to not to hurt availability of the Raft group until
it catches up with the Raft group leader.

- Opt-in deduplication mechanism via implementation of the 
[Implementing Linearizability at Large Scale and Low Latency](https://dl.acm.org/doi/10.1145/2815400.2815416) 
paper. Currently, one can implement deduplication inside his custom 
`StateMachine` implementation. I would like to offer a generic and opt-in 
solution by MicroRaft.
 
- Witness replicas possibly via implementation of the 
[Pirogue, a lighter dynamic version of the Raft distributed consensus algorithm](https://dl.acm.org/doi/10.1109/PCCC.2015.7410281) 
paper. Witness replicas participate in quorum calculations but do not keep any
state for `StateMachine` to reduce the storage overhead.
