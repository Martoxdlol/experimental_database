# Mental thoughts

Replication:
- normal setup 3 replicas

What happens when a replica goes down?

I think there must be always a system when the instances connect all to all and can detect very fast when a instance is unresponsive.

If a single replica go down (not primary), the cluster should continue to operate and start skipping that replica. But only if it is sure that enough time has passed and the replica is really down. We want to avoid inconsistent reads.

If a instance becomes alone (or can reach less than half of the cluster), it should stop accepting any operation after a timeout.

If the primary becomes alone, it should stop responding the same as other replicas.

The primary to be able to commit AND RESPOND TO THE CLIENT need:
- to receive commit ack from all oline replicas
- the online replicas should be more than half of the cluster (including itself - quorum)

Timeouts times:

we need to be sure the replicas are down to confirm/reject a transaction.
If the primary waits long enough and doesn't receive quorum acks, it should crash/stop/enter in a hold state.
In this case, we need to make sure the changes after visible_ts are rolled back. Please review the recovery process to make sure this is the case. It is probably the only case of undo. We could just run a vacuum and restart the db or something similar.

Instances should stop accepting new transaction if they lost quorum, this time should be shorter than the time the primary waits for acks, to avoid accepting transaction with potentially inconsistent reads (old data).