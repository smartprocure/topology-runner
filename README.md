# Topology Runner

## runTopology

```typescript
runTopology(spec: Spec, inputDag: DAG, options?: Options) => Response

type Response = {
  emitter: EventEmitter<Events,any>,
  promise: Promise<Snapshot>,
  getSnapshot: () => Snapshot
}
```

Run a topology consisting of a DAG (directed acyclic graph) and a spec,
which is a list of nodes that corresponds to the nodes in the DAG.

Nodes have a `run` fn that takes an object with the following shape:

```typescript
interface RunInput {
  resources: Record<string, any>
  data: any
  updateState: UpdateState
  state?: any
  signal: AbortSignal
  meta?: any
}
```

A list of named `resources` will be lazy-loaded and passed, if requested.
`data` will be the initial data passed via `options.data` for nodes with no
dependencies or an array of the outputs of the dependencies.

The flow of a DAG begins with nodes with no dependencies. More generally,
when a node's dependencies are met it will be run. Data does does not flow incrementally.
A node must complete in entirety before a node that depends on it will run.

If a node throws an error it will be caught and no further processing on that
node will be done. Parallel nodes will continue to run until they either complete
or throw an error.

An event emitter emits a new "data" snapshot every time a node starts, completes, errors,
updates its state. The final snapshot is not emitted. This will be returned by the promise.
An "error" or "done" event will be emitted when the DAG either fails to complete
or sucessfully completes. Note that the outputted snapshot is mutated internally for
efficiency and should not be modified.

```typescript
import { runTopology, DAG, Spec } from 'topology-runner'
import { setTimeout } from 'node:timers/promises'

const dag: DAG = {
  api: { deps: [] },
  details: { deps: ['api'] },
  attachments: { deps: ['api'] },
  writeToDB: { deps: ['details', 'attachments'] },
}

const spec: Spec = {
  resources: {
    elasticCloud: {
      init: async () => 'elastic',
    },
    mongoDb: {
      init: async () => 'mongo',
    },
    config: {
      init() {
        return {
          detailsHost: 'https://getsomedetails.org',
          attachmentsHost: 'https://getsomeattachments.com',
        }
      },
    },
  },
  nodes: {
    api: {
      run: async () => [1, 2, 3],
    },
    details: {
      run: async ({ data, state, resources, updateState }) => {
        data = data.flat()
        const ids: number[] = state ? data.slice(state.index + 1) : data
        const output: Record<number, string> = state ? state.output : {}

        for (let i = 0; i < ids.length; i++) {
          const id = ids[i]
          // Simulate work
          await setTimeout(10)
          // Real world scenario below
          // const description = await fetch(resources.detailsHost)
          output[id] = `description ${id}`
          // Update the state for resume scenario
          updateState({ index: i, output })
        }
        return output
      },
      resources: ['config'],
    },
    attachments: {
      run: async ({ data, state, updateState }) => {
        data = data.flat()
        const ids: number[] = state ? data.slice(state.index + 1) : data
        const output: Record<number, string> = state ? state.output : {}

        for (let i = 0; i < ids.length; i++) {
          const id = ids[i]
          // Simulate work
          await setTimeout(8)
          // Real world scenario below
          // const attachment = await fetch(resources.attachmentsHost)
          output[id] = `file${id}.jpg`
          // Update the state for resume scenario
          updateState({ index: i, output })
        }
        return output
      },
      resources: ['config'],
    },
    writeToDB: {
      // Time out after 5 minutes
      // Abort signal will abort below causing the promise to reject
      timeout: 1000 * 60 * 5,
      run: async ({ data, resources, state, updateState, signal }) => {
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
          // await resources.mongo.collection('someColl').insertOne(doc)
          // Update the state for resume scenario
          updateState({ index: i })
        }
      },
      resources: ['mongo'],
    },
  },
}

const { emitter, promise, getSnapshot } = runTopology(spec, dag)

const persistSnapshot = (snapshot) => {
  // Could be Redis, MongoDB, etc.
  // writeToDataStore(snapshot)
  console.dir(snapshot, { depth: 10 })
}

// Persist to a datastore for resuming. See below.
emitter.on('data', persistSnapshot)

try {
  // Wait for the topology to finish
  await promise
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
const dag: DAG = {
  downloadFile: { deps: [] },
  processFile: { deps: ['download'] },
}

// Using includeNodes
runTopology(spec, dag, { includeNodes: ['processFile'], data: ['123', '456'] })
// Using excludeNodes
runTopology(spec, dag, { excludeNodes: ['downloadFile'], data: ['123', '456'] })
```

## resumeTopology

```typescript
resumeTopology(spec: Spec, snapshot: Snapshot) => Response

type Response = {
  emitter: EventEmitter<Events,any>,
  promise: Promise<Snapshot>,
  getSnapshot: () => Snapshot
}
```

Allows you to resume a topology from a previously emitted snapshot.
Each node should maintain its state via the `updateState` callback.

```typescript
import { resumeTopology } from 'topology-runner'

const { emitter, promise } = resumeTopology(spec, snapshot)
await promise
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
