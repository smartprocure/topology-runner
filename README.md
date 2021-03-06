# Topology Runner

## runTopology

```typescript
runTopology(spec: Spec, options?: Options) => Response

type Response = {
  start(): Promise<void>
  stop(): void
  emitter: EventEmitter<Events, any>
  getSnapshot(): Snapshot
}
```

Run a topology consisting of a DAG (directed acyclic graph).

Nodes have a `run` fn that takes an object with the following shape:

```typescript
interface RunInput {
  data: any
  updateState: UpdateState
  state?: any
  context?: any
  node: string
  signal: AbortSignal
}
```

The flow of a DAG begins with nodes with no dependencies. More generally,
when a node's dependencies are met it will be run. Data does does not flow incrementally.
A node must complete in entirety before a node that depends on it will run.

If a node throws an error it will be caught and no further processing on that
node will be done. Parallel nodes will continue to run until they either complete
or throw an error.

An event emitter emits a new "data" snapshot every time a node starts, completes, errors, or
updates its state. Use `getSnapshot` to get the final snapshot, regardless of whether the
topology fails or succeeds. An "error" or "done" event will be emitted when the DAG either
fails to complete or sucessfully completes. Note that the outputted snapshot is mutated internally for
efficiency and should not be modified.

To gracefully shut down a topology call the `stop` function and handle the abort signal
in your run functions by throwing an exception.

```typescript
import { runTopology, Spec } from 'topology-runner'
import { setTimeout } from 'node:timers/promises'

const spec: Spec = {
  api: {
    deps: [],
    run: async () => [1, 2, 3],
  },
  details: {
    deps: ['api'],
    run: async ({ data, state, updateState }) => {
      data = data.flat()
      const ids: number[] = state ? data.slice(state.index + 1) : data
      const output: Record<number, string> = state ? state.output : {}

      for (let i = 0; i < ids.length; i++) {
        const id = ids[i]
        // Simulate work
        await setTimeout(10)
        // Real world scenario below
        // const description = await fetch(someUrl)
        output[id] = `description ${id}`
        // Update the state for resume scenario
        updateState({ index: i, output })
      }
      return output
    },
  },
  attachments: {
    deps: ['api'],
    run: async ({ data, state, updateState }) => {
      data = data.flat()
      const ids: number[] = state ? data.slice(state.index + 1) : data
      const output: Record<number, string> = state ? state.output : {}

      for (let i = 0; i < ids.length; i++) {
        const id = ids[i]
        // Simulate work
        await setTimeout(8)
        // Real world scenario below
        // const attachment = await fetch(someUrl)
        output[id] = `file${id}.jpg`
        // Update the state for resume scenario
        updateState({ index: i, output })
      }
      return output
    },
  },
  writeToDB: {
    deps: ['details', 'attachments'],
    // Time out after 5 minutes
    // Abort signal will abort below causing the promise to reject
    timeout: 1000 * 60 * 5,
    run: async ({ data, state, updateState, signal }) => {
      const [details, attachments] = data
      const keys = Object.keys(details)
      const ids = state ? keys.slice(state.index + 1) : keys

      for (let i = 0; i < ids.length; i++) {
        // Throw if timeout occurred
        if (signal.aborted) {
          throw new Error('Timed out')
        }

        // Simulate work
        await setTimeout(50)
        const id = ids[i]
        const detail = details[id]
        const attachment = attachments[id]
        const doc = { detail, attachment }
        // Write to datastore
        // await mongo.collection('someColl').insertOne(doc)
        // Update the state for resume scenario
        updateState({ index: i })
      }
    },
  },
}

const { start, emitter, getSnapshot } = runTopology(spec)

const persistSnapshot = (snapshot) => {
  // Could be Redis, MongoDB, etc.
  // writeToDataStore(snapshot)
  console.dir(snapshot, { depth: 10 })
}

// Persist to a datastore for resuming. See below.
emitter.on('data', persistSnapshot)

try {
  // Wait for the topology to finish
  await start()
} finally {
  // Persist the final snapshot
  await persistSnapshot(getSnapshot())
}
```

A successful run of the above will produce a snapshot that looks like this:

```json
{
  "status": "completed",
  "started": "2022-05-20T17:16:48.531Z",
  "dag": {
    "api": { "deps": [] },
    "details": { "deps": ["api"] },
    "attachments": { "deps": ["api"] },
    "writeToDB": { "deps": ["details", "attachments"] }
  },
  "data": {
    "api": {
      "started": "2022-05-20T17:16:48.532Z",
      "input": [],
      "status": "completed",
      "output": [1, 2, 3],
      "finished": "2022-05-20T17:16:48.533Z"
    },
    "details": {
      "started": "2022-05-20T17:16:48.534Z",
      "input": [[1, 2, 3]],
      "status": "completed",
      "state": {
        "index": 2,
        "output": {
          "1": "description 1",
          "2": "description 2",
          "3": "description 3"
        }
      },
      "output": {
        "1": "description 1",
        "2": "description 2",
        "3": "description 3"
      },
      "finished": "2022-05-20T17:16:48.566Z"
    },
    "attachments": {
      "started": "2022-05-20T17:16:48.534Z",
      "input": [[1, 2, 3]],
      "status": "completed",
      "state": {
        "index": 2,
        "output": {
          "1": "file1.jpg",
          "2": "file2.jpg",
          "3": "file3.jpg"
        }
      },
      "output": {
        "1": "file1.jpg",
        "2": "file2.jpg",
        "3": "file3.jpg"
      },
      "finished": "2022-05-20T17:16:48.562Z"
    },
    "writeToDB": {
      "started": "2022-05-20T17:16:48.567Z",
      "input": [
        {
          "1": "description 1",
          "2": "description 2",
          "3": "description 3"
        },
        {
          "1": "file1.jpg",
          "2": "file2.jpg",
          "3": "file3.jpg"
        }
      ],
      "status": "completed",
      "state": {
        "index": 2
      },
      "finished": "2022-05-20T17:16:48.722Z"
    }
  },
  "finished": "2022-05-20T17:16:48.722Z"
}
```

### Running a subset of a DAG

Sometimes you might want to skip one or more nodes in a DAG.
Say, for example, the first node downloads a file and the second
node processes that file. You may want to reprocess the file without
downloading it again. To do that you can use either the `includeNodes` or
`excludeNodes` option with some input `data`.

The computed DAG after either including or excluding nodes will be outputted
with the snapshot, making it easy to resume that topology.

```typescript
// Using includeNodes
runTopology(spec, { includeNodes: ['processFile'], data: ['123', '456'] })
// Using excludeNodes
runTopology(spec, { excludeNodes: ['downloadFile'], data: ['123', '456'] })
```

## resumeTopology

```typescript
resumeTopology(spec: Spec, snapshot: Snapshot) => Response
```

Allows you to resume a topology from a previously emitted snapshot.
Each node should maintain its state via the `updateState` callback.

```typescript
import { resumeTopology } from 'topology-runner'

const { start, emitter } = resumeTopology(spec, snapshot)
await start()
```

Below is an example snapshot where an error occurred. The DAG
can be rerun, resuming where a node did not complete. In this example,
`api` and `details` will NOT be rerun, but `attachments` would. See tests
for a resume node design pattern.

```json
{
  "status": "errored",
  "started": "2022-05-20T14:47:47.372Z",
  "dag": {
    "api": { "deps": [] },
    "details": { "deps": ["api"] },
    "attachments": { "deps": ["api"] },
    "writeToDB": { "deps": ["details", "attachments"] }
  },
  "data": {
    "api": {
      "started": "2022-05-20T14:47:47.373Z",
      "input": [],
      "status": "completed",
      "output": [1, 2, 3],
      "finished": "2022-05-20T14:47:47.373Z"
    },
    "details": {
      "started": "2022-05-20T14:47:47.373Z",
      "input": [[1, 2, 3]],
      "status": "completed",
      "output": {
        "1": "description 1",
        "2": "description 2",
        "3": "description 3"
      },
      "finished": "2022-05-20T14:47:47.373Z"
    },
    "attachments": {
      "started": "2022-05-20T14:47:47.373Z",
      "input": [[1, 2, 3]],
      "status": "errored",
      "state": {
        "index": 0,
        "output": {
          "1": "file1.jpg",
          "2": "file2.jpg"
        }
      },
      "finished": "2022-05-20T14:47:47.374Z"
    }
  },
  "error": "Failed processing id: 2",
  "finished": "2022-05-20T14:47:47.374Z"
}
```
