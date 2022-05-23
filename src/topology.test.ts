import { describe, expect, test } from '@jest/globals'
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
  TopologyError,
} from './topology'
import { DAG, RunFn, Snapshot, Spec } from './types'

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

describe('filterDAG', () => {
  test('exclude nodes', () => {
    expect(filterDAG(dag, { excludeNodes: ['api'] })).toEqual({
      details: { deps: [] },
      attachments: { deps: [] },
      writeToDB: { deps: ['details', 'attachments'] },
    })
  })
  test('include nodes', () => {
    expect(filterDAG(dag, { includeNodes: ['details', 'writeToDB'] })).toEqual({
      details: { deps: [] },
      writeToDB: { deps: ['details'] },
    })
  })
  test('no options', () => {
    expect(filterDAG(dag)).toEqual(dag)
  })
})

describe('getNodesReadyToRun', () => {
  const dag = {
    api: { deps: [] },
    details: { deps: ['api'] },
    attachments: { deps: ['api'] },
    writeToDB: { deps: ['details', 'attachments'] },
  }
  test('deps met - exclude completed', () => {
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
    expect(nodes).toEqual(['details', 'attachments'])
  })
  test('deps met - exclude completed and running', () => {
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
      details: {
        status: 'running',
        started: new Date('2022-01-01T12:00:00Z'),
        input: [['123', '456']],
      },
    })
    expect(nodes).toEqual(['attachments'])
  })
  test('empty deps', () => {
    const nodes = getNodesReadyToRun(dag, {})

    expect(nodes).toEqual(['api'])
  })
})

describe('initResources', () => {
  test('resources initialized', async () => {
    expect(
      await initResources(spec, ['elasticCloud', 'mongoDb', 'config'])
    ).toEqual({
      elasticCloud: 'elastic',
      mongoDb: 'mongo',
      config: { host: 'localhost' },
    })
  })
})

describe('getInputData', () => {
  test('single dep', () => {
    const input = getInputData(dag, 'details', {
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
    expect(input).toEqual([['123', '456']])
  })
  test('multiple deps', () => {
    const input = getInputData(dag, 'writeToDB', {
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
    expect(input).toEqual([
      { 123: { description: 'foo' } },
      { 123: { file: 'foo.jpg' } },
    ])
  })
  test('resume scenario', () => {
    const input = getInputData(dag, 'api', {
      api: {
        status: 'pending',
        input: {
          startDate: '2020-01-01',
          endDate: '2020-12-31',
        },
        state: '2020-04-01',
      },
    })
    expect(input).toEqual({
      startDate: '2020-01-01',
      endDate: '2020-12-31',
    })
  })
})

describe('initData', () => {
  test('data passed', () => {
    expect(initSnapshotData(dag, { data: [1, 2, 3] })).toEqual({
      api: { input: [1, 2, 3] },
    })
  })
  test('data empty', () => {
    expect(initSnapshotData(dag, {})).toEqual({})
  })
})

describe('runTopology', () => {
  test('bad arguments', () => {
    const dag = {
      api: { deps: [] },
      details: { deps: ['api'] },
      attachments: { deps: ['api'] },
      writeToDB: { deps: ['details', 'attachments'] },
    }
    const spec: Spec = {
      nodes: {
        api: {
          run: async () => [1, 2, 3],
        },
        attachments: {
          run: async () => 2,
        },
      },
    }
    expect(() => {
      runTopology(spec, dag)
    }).toThrow(
      new TopologyError(
        'Missing the following nodes in spec: details, writeToDB'
      )
    )
  })
  test('completed', async () => {
    const { promise } = runTopology(spec, dag)
    const snapshot = await promise
    expect(snapshot).toMatchObject({
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
})

describe('getResumeSnapshot', () => {
  test('transform snapshot for resumption', () => {
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
      error: 'Failed processing id: 1',
    }
    const snapshot = getResumeSnapshot(errorSnapshot)
    expect(snapshot).toMatchObject({
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
})

describe('resumeTopology', () => {
  let attempt = 1
  const attachmentsRun: RunFn = async ({ data, state, updateStateFn }) => {
    // Flatten
    data = data.flat()
    // Start from next element if resume scenario
    const ids: number[] = state ? data.slice(state.index + 1) : data
    const output: Record<number, string> = state ? state.output : {}
    for (let i = 0; i < ids.length; i++) {
      const id = ids[i]
      output[id] = `file${id}.jpg`
      // Simulate error while processing second element.
      // Error occurs the first time the fn is called.
      if (i === 1 && attempt++ === 1) {
        throw new Error(`Failed processing id: ${id}`)
      }
      // Successfully processed so record state
      updateStateFn({ index: i, output })
    }
    return output
  }
  const modifiedSpec = _.set('nodes.attachments.run', attachmentsRun, spec)

  test('resume after initial error', async () => {
    const { promise, getSnapshot } = runTopology(modifiedSpec, dag)
    await expect(promise).rejects.toThrow('Failed processing id: 2')
    const snapshot = getSnapshot()
    expect(snapshot).toMatchObject({
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
          state: {
            index: 0,
            output: {
              '1': 'file1.jpg',
            },
          },
        },
      },
      error: 'Failed processing id: 2',
    })
    const { promise: resumeProm } = await resumeTopology(modifiedSpec, snapshot)
    const resumeSnapshot = await resumeProm
    expect(resumeSnapshot).toMatchObject({
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
})
