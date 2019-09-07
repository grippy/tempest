# Topology

As previously stated: A Topology reads messages from a source and moves them into a pipeline of tasks for processing.

## Configuration

A topology has various configuration options:

- Message failure policy (best effort, retry, drop) on how to handle message failure
- Message timeout (trigger message failure if a message isn't acknowledged before this interval)
- Metric targets
- Metric flush interval
- Graceful shutdown delay

Take a look at the `Topology` trait in the next section for more details.