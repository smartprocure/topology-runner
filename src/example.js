/* eslint-disable */

let spec = {
  resources: {
    elasticCloud: {
      init: () => {},
    },
    mongoDb: {
      init: () => {},
    },
  },
  nodes: {
    api: {
      run: async ({ resources, data, updateStateFn, state }) => {
        // Flatten the input data. Assumes all sources have the same shape
        // [['http://foo.com', 'https://bar.org'], ['https://site.net']]
        data = data.flat()
        // Order data
        data.sort()
        // Get a subset of the data if resuming
        const urls = state ? data.slice(state) : data
        // Process data
        const ids = []
        urls.forEach(async (url, index) => {
          const res = await fetchFromService(url)
          await writeToDB(res)
          ids.push(res.id)
          // Update state
          updateStateFn(index)
        })
        // Output data for next node
        return ids
      },
      resources: [],
      timeout: 5000,
    },
    details: {
      run: ({ resources, data }) => {},
      resources: ['elasticCloud'],
      timeout: 10000,
    },
  },
}

const dag = {
  // input: path, storage type (gcs, filesystem)
  // output: list of paths
  // artifacts: files in the storage
  downloadFiles: { deps: [] },

  // input: path, storage type (gcs, filesystem)
  // output: mongo id pointing to the artifact
  // artifacts: list of ids (size of 400k)
  processFile: { deps: ['downloadFiles'] },

  // input: start,end
  // output: list of ids
  // artifacts: records in mongo
  api: { deps: [] },

  // input: list of ids
  // output: list of ids
  // artifacts: records in mongo, list of ids
  // state: { cursor: id }
  history: { deps: ['api', 'processFile'] },

  // input: list of ids
  // output: list of ids
  // artifacts: records in mongo
  // state: { cursor: id }
  downloadStuff: { deps: ['api', 'history', 'processFile'] },

  // input: list of ids
  // output: list of ids
  // artifacts: records in mongo
  // state: { cursor: id }
  details: { deps: ['downloadStuff'] },

  // input: list of ids
  // output: list of ids
  // artifacts: records in mongo
  // state: { cursor: id }
  attachments: { deps: ['downloadStuff'] },

  // input: list of ids
  // artifacts: records in mongo
  // state: { cursor: id }
  mongo: { deps: ['details', 'attachments'] },
}

const { emitter, promise } = runTopology(
  {
    resources: {
      foo: {
        init: async () => {
          return 3
        },
      },
    },
    nodes: {
      api: {
        async run() {},
      },
    },
  },
  {
    api: { deps: [] },
    details: { deps: ['api'] },
    history: { deps: ['api'] },
  },
  { excludeNodes: ['api'], data: {} }
)

resumeTopology(spec, {
  status: 'running',
  dag: {
    api: { deps: [] },
    details: { deps: ['api'] },
    history: { deps: ['api'] },
  },
  data: {
    api: {
      status: 'completed',
      started: '2022-01-01T12:00:00Z',
      finished: '2022-01-01T12:05:00Z',
      input: {
        startDate: '2020-01-01',
        endDate: '2020-12-31',
      },
      state: {
        date: '2020-04-01',
        id: '123',
      },
      output: ['123', '456'],
    },
    details: {
      status: 'running',
      state: {
        id: 3,
      },
      input: [['123', '456']],
    },
  },
})

/**
 * Example job
 */
const perform = async (msg) => {
  const isRedelivery = msg.deliveryCount > 1
  const { emitter, promise } = isRedelivery
    ? resumeTopology(spec, await loadSnapshotFromMongo(msg))
    : runTopology(spec, dag, options)
  const persistSnapshot = (snapshot) => {
    // Let NATS know we're working
    msg.working()
    // write to mongo
    persistToMongo(msg, snapshot)
  }
  emitter.on('data', persistSnapshot)
  emitter.on('done', persistSnapshot)
  return promise
}

// Feathers service
/*
POST /topology
{
  topologyId: 'samGov',
  includeNodes: ['api'],
  data: {}
}
GET /topology?id=samGov&status=running
*/

// MongoDB topology collection record
let mongo = {
  // MongoDB only
  topologyId: 'samGov',
  // MongoDB only
  stream: 'whatever',
  // MongoDB only
  streamSequence: 123,
  status: 'running',
  // This is a snapshot of the dag when processing started.
  // It is the computed dag after taking into account includeNodes or excludeNodes.
  dag: {
    api: { deps: [] },
    details: { deps: ['api'] },
    history: { deps: ['api'] },
  },
  data: {
    api: {
      status: 'completed',
      started: '2022-01-01T12:00:00Z',
      finished: '2022-01-01T12:05:00Z',
      input: {
        startDate: '2020-01-01',
        endDate: '2020-12-31',
      },
      state: {
        date: '2020-04-01',
        id: '123',
      },
      output: ['123', '456'],
    },
    details: {
      status: 'running',
      state: {
        whatever: 3,
      },
      // Input should always be a ref when there is a previous stage with an output
      // Output should always be a ref when input === output
      input: [['123', '456']],
    },
  },
}
