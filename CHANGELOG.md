# 0.9.0

* Added two new node types: `branching` and `suspension`. The former
allows for branching logic and the latter allows for suspending execution.

# 0.8.0

* BREAKING CHANGE: Changed `promise` to `start` fn.

# 0.7.0

* Spec includes `deps` so you can just pass one object.
* Snapshot data includes `deps`. Removed `dag`.
* Get error stacktrace if available.
* Wrap initial input data in an array to match other nodes.
* Removed `resources`.

# 0.6.0

* Handle empty snapshot when resuming.

# 0.5.0

* Add `stop` fn to abort the topology.

# 0.4.0

* Remove `timeout` since this library probably isn't the best place for it.

# 0.3.0

* `runTopology` and `resumeTopology` return `Promise<void>` from
the `promise` property.
* Don't emit `data` when finished.
* Changed `meta` to `context`. This is just a blob that is passed
to `runTopology` and `resumeTopology` without needing to be serialized.

# 0.2.0

* Always emit `data`.

# 0.1.0

* Nodes run to completion in parallel with failed nodes.

# 0.0.6

* Added `meta` option for `runTopology`.

# 0.0.5

* Changed `updateStateFn` to `updateState`.

# 0.0.4

* Added `cleanup` fn for resources that will run when the topology terminates.

# 0.0.3

* Consist result type for `resumeTopology`.

# 0.0.2

* Re-export types in index.ts.
* Switched over to Jest for testing.

# 0.0.1

* Initial release
