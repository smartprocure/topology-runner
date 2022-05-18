let topology = {
  id: 'samGov',
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
      run: ({ resources, data, updateStateFn }) => {},
      resources: [],
      timeout: 5000,
    },
    details: {
      run: ({ resources, data }) => {},
      resources: ['elasticCloud'],
      timeout: 10000,
    },
  },
  dag: {
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
  },
}

runTopology({
  id: 'foo',
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
  dag: {
    api: { deps: [] },
    details: { deps: ['api'] },
    history: { deps: ['api'] },
  },
})


// Feathers service
/*
POST /topology
{
  id: 'samGov',
  includeNodes: ['api'],
  data: {}
}
GET /topology?id=samGov&status=running
*/

// MongoDB topology collection record
let mongo = {
  id: 'samGov',
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
      completed: '2022-01-01T12:05:00Z',
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
      input: { api: ['123', '456'] },
    },
  },
}
