# Topology Runner

## runTopology

Run a topology consisting of a DAG (directed acyclic graph) and a spec,
which is a list of nodes that corresponds to the nodes in the DAG.

Nodes have a `run` fn that takes an object with the following shape:

```typescript
interface RunInput {
  resources: Record<string, any>
  data: any
  updateStateFn: UpdateStateFn
  state?: any
  signal: AbortSignal
}
```

A list of named `resources` will be lazy-loaded and passed, if requested.
`data` will be the initial data passed via `options.data` for nodes with no
dependencies or an array of the outputs of the dependencies.

The flow of a DAG begins with nodes with no dependencies. More generally,
when a node's dependencies are met it will be run. Data does does not flow incrementally.
A node must complete in entirety before a node that depends on it will run.

```typescript
import { DAG, Spec } from 'topology-runner/dist/types'

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
        return { host: 'localhost' }
      },
    },
  },
  nodes: {
    api: {
      run: async () => [1, 2, 3],
    },
    details: {
      run: async ({ data }) => {
        const ids: number[] = data[0]
        return ids.reduce((acc, n) => _.set(n, `description ${n}`, acc), {})
      },
    },
    attachments: {
      run: async ({ data }) => {
        const ids: number[] = data[0]
        return ids.reduce((acc, n) => _.set(n, `file${n}.jpg`, acc), {})
      },
    },
    writeToDB: {
      run: async ({ data, resources }) => {
        await resources.mongo.collection('someColl').insertMany(data)
      },
      resources: ['mongo', 'config'],
    },
  },
}

const { emitter, promise } = runTopology(spec, dag)

const persistSnapshot = (snapshot) => {
  // Could be Redis, MongoDB, etc.
  writeToDataStore(snapshot)
}

// Persist to a destore for resuming. See below.
emitter.on('data', persistSnapshot)
emitter.on('done', persistSnapshot)

const snapshot = await promise
```

A successful run of the above will produce a snapshot that looks like this:

```json
{
  "status": "completed",
  "started": "2022-05-20T14:44:50.337Z",
  "dag": {
    "api": { "deps": [] },
    "details": { "deps": [ "api" ] },
    "attachments": { "deps": [ "api" ] },
    "writeToDB": { "deps": [ "details", "attachments" ] }
  },
  "data": {
    "api": {
      "started": "2022-05-20T14:44:50.338Z",
      "input": [],
      "status": "completed",
      "output": [ 1, 2, 3 ],
      "finished": "2022-05-20T14:44:50.339Z"
    },
    "details": {
      "started": "2022-05-20T14:44:50.339Z",
      "input": [ [ 1, 2, 3 ] ],
      "status": "completed",
      "output": {
        "1": "description 1",
        "2": "description 2",
        "3": "description 3"
      },
      "finished": "2022-05-20T14:44:50.339Z"
    },
    "attachments": {
      "started": "2022-05-20T14:44:50.339Z",
      "input": [ [ 1, 2, 3 ] ],
      "status": "completed",
      "output": {
        "1": "file1.jpg",
        "2": "file2.jpg",
        "3": "file3.jpg"
      },
      "finished": "2022-05-20T14:44:50.340Z"
    },
    "writeToDB": {
      "started": "2022-05-20T14:44:50.340Z",
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
      "output": null,
      "finished": "2022-05-20T14:44:50.340Z"
    }
  },
  "finished": "2022-05-20T14:44:50.340Z"
}
```

## resumeTopology

Allows you to resume a topology from a previously emitted snapshot.
Each node should maintain its state via the `updateStateFn` callback.

```typescript
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
    "details": { "deps": [ "api" ] },
    "attachments": { "deps": [ "api" ] },
    "writeToDB": { "deps": [ "details", "attachments" ] }
  },
  "data": {
    "api": {
      "started": "2022-05-20T14:47:47.373Z",
      "input": [],
      "status": "completed",
      "output": [ 1, 2, 3 ],
      "finished": "2022-05-20T14:47:47.373Z"
    },
    "details": {
      "started": "2022-05-20T14:47:47.373Z",
      "input": [ [ 1, 2, 3 ] ],
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
      "input": [ [ 1, 2, 3 ] ],
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
