# internal/reconciler

`Runnable` and `Reconcilable` are the two lifecycles the applier starts.

```mermaid
flowchart TB
    apply["ApplyReconciler"]
    delete["DeleteReconciler"]
    reconcilable["reconciler.Reconcilable"]
    serial["serial.Runnable"]
    read["readdesire.Controller"]
    refresher["discovery.Refresher"]
    leader["leadership.Runnable"]
    runnable["reconciler.Runnable"]
    start["startRunnables"]

    apply -->|"implements"| reconcilable
    delete -->|"implements"| reconcilable
    reconcilable -->|"serial.New stores them"| serial

    serial -->|"leader only"| leader
    read -->|"leader only"| leader
    leader -->|"implements"| runnable
    refresher -->|"implements"| runnable
    runnable -->|"Start"| start
```

`serial.New` returns `*serial.Runnable`. `readdesire.New` returns `*Controller`.
Both implement `Runnable`, but `leadership.Runnable` starts them only after acquiring
the management-cluster Lease. Losing the Lease cancels their contexts and waits for
both to stop before returning an error, causing the service to exit.
`startRunnables` starts the election wrapper and `discovery.Refresher` on every
replica. `serial.Runnable.Start` calls `RunOnce` on apply, then delete.
