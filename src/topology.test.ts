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
  cleanupResources,
} from './topology'
import { DAG, RunFn, Snapshot, Spec } from './types'
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
  test('resume - pending', () => {
    const nodes = getNodesReadyToRun(dag, {
      api: {
        status: 'pending',
        started: new Date('2022-01-01T12:00:00Z'),
        input: {
          startDate: '2020-01-01',
          endDate: '2020-12-31',
        },
      },
    })
    expect(nodes).toEqual(['api'])
  })
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
  test('deps met - exclude completed and errored', () => {
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
        status: 'errored',
        started: new Date('2022-01-01T12:00:00Z'),
        input: [['123', '456']],
      },
    })
    expect(nodes).toEqual(['attachments'])
  })
  test('deps not met', () => {
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
        status: 'completed',
        started: new Date('2022-01-01T12:00:00Z'),
        input: [['123', '456']],
        output: { '123': 'foo', '456': 'bar' },
      },
      attachments: {
        status: 'errored',
        started: new Date('2022-01-01T12:00:00Z'),
        input: [['123', '456']],
      },
    })
    expect(nodes).toEqual([])
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

describe('cleanupResources', () => {
  test('clean up initialized resources', async () => {
    const cleanedUp: any[] = []
    const cleanup = (x: any) => {
      cleanedUp.push(x)
    }

    const spec: Spec = {
      resources: {
        elasticCloud: {
          init: async () => 'elastic',
          cleanup: async (x: any) => cleanup(x),
        },
        mongoDb: {
          init: async () => 'mongo',
          cleanup: (x: any) => cleanup(x),
        },
        config: {
          init() {
            return { host: 'localhost' }
          },
        },
      },
      nodes: {},
    }

    const initialized = await initResources(spec, [
      'elasticCloud',
      'mongoDb',
      'config',
    ])
    await cleanupResources(spec, initialized)
    expect(cleanedUp).toEqual(['elastic', 'mongo'])
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
  test('nodes receive expected input', async () => {
    const dag = {
      api: { deps: [] },
      details: { deps: ['api'] },
    }
    const spec: Spec = {
      resources: {
        elasticCloud: {
          init: async () => 'elastic',
        },
        mongoDb: {
          init: async () => 'mongo',
        },
      },
      nodes: {
        api: {
          run: async ({ data, resources, meta }) => ({ data, resources, meta }),
          resources: ['elasticCloud'],
        },
        details: {
          run: async ({ data, resources, meta }) => ({ data, resources, meta }),
          resources: ['mongoDb'],
        },
      },
    }
    const data = [1, 2, 3]
    const meta = { launchMissleCode: 1234 }
    const { promise, getSnapshot } = runTopology(spec, dag, { data, meta })
    await promise
    expect(getSnapshot()).toMatchObject({
      status: 'completed',
      dag: { api: { deps: [] }, details: { deps: ['api'] } },
      data: {
        api: {
          input: [1, 2, 3],
          status: 'completed',
          output: {
            data: [1, 2, 3],
            resources: { elasticCloud: 'elastic' },
            meta: { launchMissleCode: 1234 },
          },
        },
        details: {
          input: [
            {
              data: [1, 2, 3],
              resources: { elasticCloud: 'elastic' },
              meta: { launchMissleCode: 1234 },
            },
          ],
          status: 'completed',
          output: {
            data: [
              {
                data: [1, 2, 3],
                resources: { elasticCloud: 'elastic' },
                meta: { launchMissleCode: 1234 },
              },
            ],
            resources: { mongoDb: 'mongo' },
            meta: { launchMissleCode: 1234 },
          },
        },
      },
      meta: { launchMissleCode: 1234 },
    })
  })
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
  test('timeout', async () => {
    const dag = {
      api: { deps: [] },
      details: { deps: ['api'] },
    }
    const spec: Spec = {
      nodes: {
        api: {
          run: async ({ signal }) => {
            await setTimeout(500)
            if (signal.aborted) {
              throw new Error('Timeout')
            }
          },
          timeout: 250,
        },
        details: {
          run: async () => 'foo',
        },
      },
    }
    const { promise } = runTopology(spec, dag)
    await expect(promise).rejects.toThrow('Errored nodes: ["api"]')
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
  const attachmentsRun: RunFn = async ({ data, state, updateState }) => {
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
      updateState({ index: i, output })
    }
    return output
  }
  const modifiedSpec = _.set('nodes.attachments.run', attachmentsRun, spec)

  test('resume after initial error', async () => {
    const { promise, getSnapshot } = runTopology(modifiedSpec, dag)
    await expect(promise).rejects.toThrow('Errored nodes: ["attachments"]')
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
          error: 'Error: Failed processing id: 2',
          state: {
            index: 0,
            output: {
              '1': 'file1.jpg',
            },
          },
        },
      },
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
