import test from 'ava'
import _ from 'lodash/fp'
import {
  getInputData,
  filterDAG,
  getNodesReadyToRun,
  initResources,
  initSnapshotData,
  runTopology,
  resumeTopology,
  getResumeSnapshot,
} from './topology'
import { RunFn, Snapshot, Spec } from './types'

const dag = {
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
      resources: ['elasticCloud'],
    },
    details: {
      run: async ({ data }) => {
        const ids: number[] = data[0]
        return ids.reduce((acc, n) => _.set(n, `description ${n}`, acc), {})
      },
      resources: ['elasticCloud', 'mongo'],
    },
    attachments: {
      run: async ({ data }) => {
        const ids: number[] = data[0]
        return ids.reduce((acc, n) => _.set(n, `file${n}.jpg`, acc), {})
      },
      resources: [],
    },
    writeToDB: {
      run: async () => null,
      resources: ['mongo', 'config'],
    },
  },
}

test('filterDAG', (t) => {
  t.deepEqual(
    filterDAG(dag, { excludeNodes: ['api'] }),
    {
      details: { deps: [] },
      attachments: { deps: [] },
      writeToDB: { deps: ['details', 'attachments'] },
    },
    'excludes nodes'
  )
  t.deepEqual(
    filterDAG(dag, { includeNodes: ['details', 'writeToDB'] }),
    {
      details: { deps: [] },
      writeToDB: { deps: ['details'] },
    },
    'include nodes'
  )
  t.deepEqual(filterDAG(dag), dag, 'no options')
})

test('getNodesReadyToRun', (t) => {
  const dag = {
    api: { deps: [] },
    details: { deps: ['api'] },
    attachments: { deps: ['api'] },
    writeToDB: { deps: ['details', 'attachments'] },
  }
  const nodes = getNodesReadyToRun(dag, {
    api: {
      status: 'completed',
      started: new Date('2022-01-01T12:00:00Z'),
      finished: new Date('2022-01-01T12:05:00Z'),
      input: {
        startDate: '2020-01-01',
        endDate: '2020-12-31',
      },
      output: ['123', '456'],
    },
  })
  t.deepEqual(nodes, ['details', 'attachments'], 'deps met - exclude completed')
  const nodes2 = getNodesReadyToRun(dag, {
    api: {
      status: 'completed',
      started: new Date('2022-01-01T12:00:00Z'),
      finished: new Date('2022-01-01T12:05:00Z'),
      input: {
        startDate: '2020-01-01',
        endDate: '2020-12-31',
      },
      output: ['123', '456'],
    },
    details: {
      status: 'running',
      started: new Date('2022-01-01T12:00:00Z'),
      input: [['123', '456']],
    },
  })
  t.deepEqual(
    nodes2,
    ['attachments'],
    'deps met - exclude completed and running'
  )
  const nodes3 = getNodesReadyToRun(dag, {})
  t.deepEqual(nodes3, ['api'], 'empty deps')
})

test('initResources', async (t) => {
  t.deepEqual(
    await initResources(spec, ['elasticCloud', 'mongoDb', 'config']),
    { elasticCloud: 'elastic', mongoDb: 'mongo', config: { host: 'localhost' } }
  )
})

test('getInputData', (t) => {
  const input1 = getInputData(dag, 'details', {
    api: {
      status: 'completed',
      started: new Date('2022-01-01T12:00:00Z'),
      finished: new Date('2022-01-01T12:05:00Z'),
      input: {
        startDate: '2020-01-01',
        endDate: '2020-12-31',
      },
      output: ['123', '456'],
    },
  })
  t.deepEqual(input1, [['123', '456']], 'single dep')
  const input2 = getInputData(dag, 'writeToDB', {
    api: {
      status: 'completed',
      started: new Date('2022-01-01T12:00:00Z'),
      finished: new Date('2022-01-01T12:05:00Z'),
      input: {
        startDate: '2020-01-01',
        endDate: '2020-12-31',
      },
      output: ['123', '456'],
    },
    details: {
      status: 'completed',
      started: new Date('2022-01-01T12:00:00Z'),
      finished: new Date('2022-01-01T12:05:00Z'),
      input: [['123', '456']],
      output: { 123: { description: 'foo' } },
    },
    attachments: {
      status: 'completed',
      started: new Date('2022-01-01T12:00:00Z'),
      finished: new Date('2022-01-01T12:05:00Z'),
      input: [['123', '456']],
      output: { 123: { file: 'foo.jpg' } },
    },
  })
  t.deepEqual(
    input2,
    [{ 123: { description: 'foo' } }, { 123: { file: 'foo.jpg' } }],
    'multiple deps'
  )
  const input3 = getInputData(dag, 'api', {
    api: {
      status: 'pending',
      input: {
        startDate: '2020-01-01',
        endDate: '2020-12-31',
      },
      state: '2020-04-01',
    },
  })
  t.deepEqual(
    input3,
    {
      startDate: '2020-01-01',
      endDate: '2020-12-31',
    },
    'resume scenario'
  )
})

test('initData', (t) => {
  t.deepEqual(
    initSnapshotData(dag, { data: [1, 2, 3] }),
    { api: { input: [1, 2, 3] } },
    'data passed'
  )
  t.deepEqual(initSnapshotData(dag, {}), {}, 'data empty')
})

test('runTopology', async (t) => {
  const { promise } = runTopology(spec, dag)
  const snapshot = await promise
  t.like(snapshot, {
    status: 'completed',
    dag: {
      api: { deps: [] },
      details: { deps: ['api'] },
      attachments: { deps: ['api'] },
      writeToDB: { deps: ['details', 'attachments'] },
    },
    data: {
      api: {
        input: [],
        status: 'completed',
        output: [1, 2, 3],
      },
      details: {
        input: [[1, 2, 3]],
        status: 'completed',
        output: {
          1: 'description 1',
          2: 'description 2',
          3: 'description 3',
        },
      },
      attachments: {
        input: [[1, 2, 3]],
        status: 'completed',
        output: {
          1: 'file1.jpg',
          2: 'file2.jpg',
          3: 'file3.jpg',
        },
      },
      writeToDB: {
        input: [
          {
            1: 'description 1',
            2: 'description 2',
            3: 'description 3',
          },
          {
            1: 'file1.jpg',
            2: 'file2.jpg',
            3: 'file3.jpg',
          },
        ],
        status: 'completed',
        output: null,
      },
    },
  })
})

test('getResumeSnapshot', (t) => {
  const errorSnapshot: Snapshot = {
    status: 'errored',
    started: new Date('2020-01-01T00:00:00Z'),
    finished: new Date('2020-01-01T00:00:01Z'),
    dag: {
      api: { deps: [] },
      details: { deps: ['api'] },
      attachments: { deps: ['api'] },
      writeToDB: { deps: ['details', 'attachments'] },
    },
    data: {
      api: {
        started: new Date('2020-01-01T00:00:00Z'),
        finished: new Date('2020-01-01T00:00:01Z'),
        input: [],
        status: 'completed',
        output: [1, 2, 3],
      },
      details: {
        started: new Date('2020-01-01T00:00:00Z'),
        finished: new Date('2020-01-01T00:00:01Z'),
        input: [[1, 2, 3]],
        status: 'completed',
        output: {
          '1': 'description 1',
          '2': 'description 2',
          '3': 'description 3',
        },
      },
      attachments: {
        started: new Date('2020-01-01T00:00:00Z'),
        input: [[1, 2, 3]],
        status: 'errored',
        state: 0,
      },
    },
    error: new Error('Invalid id: 1'),
  }
  const snapshot = getResumeSnapshot(errorSnapshot)
  t.like(snapshot, {
    status: 'running',
    dag: {
      api: {
        deps: [],
      },
      details: {
        deps: ['api'],
      },
      attachments: {
        deps: ['api'],
      },
      writeToDB: {
        deps: ['details', 'attachments'],
      },
    },
    data: {
      api: {
        started: new Date('2020-01-01T00:00:00.000Z'),
        finished: new Date('2020-01-01T00:00:01.000Z'),
        input: [],
        status: 'completed',
        output: [1, 2, 3],
      },
      details: {
        started: new Date('2020-01-01T00:00:00.000Z'),
        finished: new Date('2020-01-01T00:00:01.000Z'),
        input: [[1, 2, 3]],
        status: 'completed',
        output: {
          '1': 'description 1',
          '2': 'description 2',
          '3': 'description 3',
        },
      },
      attachments: {
        input: [[1, 2, 3]],
        state: 0,
        status: 'pending',
      },
    },
  })
})

test.skip('resumeTopology', async (t) => {
  const testState = { attempt: 1 }
  const attachmentsRun: RunFn = async ({ data, state, updateStateFn }) => {
    // Flatten
    data = data.flat()
    // Order data
    data.sort()
    const ids: number[] = state ? data.slice(state) : data
    ids.sort()
    const obj: Record<number, string> = {}
    for (let i = 0; i < ids.length; i++) {
      updateStateFn(i)
      const id = ids[i]
      obj[id] = `file${id}.jpg`
      console.log('TEST STATE %O', testState)
      if (testState.attempt === 1) {
        console.log('THROW')
        throw new Error(`Invalid id: ${id}`)
      }
    }
    return obj
  }
  const modifiedSpec = _.set('nodes.attachments.run', attachmentsRun, spec)
  const { promise, getSnapshot } = runTopology(modifiedSpec, dag)
  /* eslint-disable-next-line */
  await promise.catch(() => {})
  const snapshot = getSnapshot()
  t.like(
    snapshot,
    {
      status: 'errored',
      dag: {
        api: { deps: [] },
        details: { deps: ['api'] },
        attachments: { deps: ['api'] },
        writeToDB: { deps: ['details', 'attachments'] },
      },
      data: {
        api: {
          input: [],
          status: 'completed',
          output: [1, 2, 3],
        },
        details: {
          input: [[1, 2, 3]],
          status: 'completed',
          output: {
            '1': 'description 1',
            '2': 'description 2',
            '3': 'description 3',
          },
        },
        attachments: {
          input: [[1, 2, 3]],
          status: 'errored',
          state: 0,
        },
      },
      error: new Error('Invalid id: 1'),
    },
    'error snapshot'
  )
  testState.attempt = 2
  const { promise: resumeProm } = await resumeTopology(modifiedSpec, snapshot)
  const resumeSnapshot = await resumeProm
  console.log(JSON.stringify(snapshot, null, 2))
  t.like(
    resumeSnapshot,
    {
      status: 'errored',
      started: '2022-05-19T21:44:29.552Z',
      dag: {
        api: { deps: [] },
        details: { deps: ['api'] },
        attachments: { deps: ['api'] },
        writeToDB: { deps: ['details', 'attachments'] },
      },
      data: {
        api: {
          started: '2022-05-19T21:44:29.552Z',
          input: [],
          status: 'completed',
          output: [1, 2, 3],
          finished: '2022-05-19T21:44:29.552Z',
        },
        details: {
          started: '2022-05-19T21:44:29.553Z',
          input: [[1, 2, 3]],
          status: 'completed',
          output: {
            '1': 'description 1',
            '2': 'description 2',
            '3': 'description 3',
          },
          finished: '2022-05-19T21:44:29.553Z',
        },
        attachments: {
          started: '2022-05-19T21:44:29.553Z',
          input: [[1, 2, 3]],
          status: 'errored',
          state: 0,
          finished: '2022-05-19T21:44:29.553Z',
        },
      },
      error: {},
      finished: '2022-05-19T21:44:29.553Z',
    },
    'resume snapshot'
  )
})
