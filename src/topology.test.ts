import { describe, expect, test } from '@jest/globals'
import _ from 'lodash/fp'
import {
  getInputData,
  filterDAG,
  getNodesReadyToRun,
  initSnapshotData,
  runTopology,
  resumeTopology,
  getResumeSnapshot,
  TopologyError,
} from './topology'
import { DAG, RunFn, Snapshot, Spec } from './types'
import timers from 'timers/promises'

const dag: DAG = {
  api: { deps: [], type: 'work' },
  details: { deps: ['api'], type: 'work' },
  attachments: { deps: ['api'], type: 'work' },
  writeToDB: { deps: ['details', 'attachments'], type: 'work' },
}

const spec: Spec = {
  api: {
    deps: [],
    run: async () => [1, 2, 3],
  },
  details: {
    deps: ['api'],
    run: async ({ data }) => {
      const ids: number[] = data[0]
      return ids.reduce((acc, n) => _.set(n, `description ${n}`, acc), {})
    },
  },
  attachments: {
    deps: ['api'],
    run: async ({ data }) => {
      const ids: number[] = data[0]
      return ids.reduce((acc, n) => _.set(n, `file${n}.jpg`, acc), {})
    },
  },
  writeToDB: {
    deps: ['details', 'attachments'],
    run: async () => null,
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
        deps: [],
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
        deps: [],
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
        deps: [],
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
        deps: ['api'],
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
        deps: [],
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
        deps: ['api'],
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
        deps: [],
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
        deps: ['api'],
        status: 'completed',
        started: new Date('2022-01-01T12:00:00Z'),
        input: [['123', '456']],
        output: { '123': 'foo', '456': 'bar' },
      },
      attachments: {
        deps: ['api'],
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

describe('getInputData', () => {
  test('single dep', () => {
    const input = getInputData(dag, 'details', {
      api: {
        deps: [],
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
        deps: [],
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
        deps: ['api'],
        status: 'completed',
        started: new Date('2022-01-01T12:00:00Z'),
        finished: new Date('2022-01-01T12:05:00Z'),
        input: [['123', '456']],
        output: { 123: { description: 'foo' } },
      },
      attachments: {
        deps: ['api'],
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
        deps: [],
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
    expect(initSnapshotData(dag, [1, 2, 3])).toEqual({
      api: { deps: [], status: 'pending', input: [[1, 2, 3]] },
      details: { deps: ['api'], status: 'pending' },
      attachments: { deps: ['api'], status: 'pending' },
      writeToDB: { deps: ['details', 'attachments'], status: 'pending' },
    })
  })
  test('data empty', () => {
    expect(initSnapshotData(dag)).toEqual({
      api: { deps: [], status: 'pending' },
      details: { deps: ['api'], status: 'pending' },
      attachments: { deps: ['api'], status: 'pending' },
      writeToDB: { deps: ['details', 'attachments'], status: 'pending' },
    })
  })
})

describe('runTopology', () => {
  test('nodes receive expected input', async () => {
    const spec: Spec = {
      api: {
        deps: [],
        run: async ({ data, context }) => ({
          data,
          context,
        }),
      },
      details: {
        deps: ['api'],
        run: async ({ data, context }) => ({
          data,
          context,
        }),
      },
    }
    const data = [1, 2, 3]
    const context = { launchMissleCode: 1234 }
    const { start, getSnapshot } = runTopology(spec, { data, context })
    await start()
    expect(getSnapshot()).toMatchObject({
      status: 'completed',
      data: {
        api: {
          input: [[1, 2, 3]],
          status: 'completed',
          output: {
            data: [[1, 2, 3]],
            context: { launchMissleCode: 1234 },
          },
        },
        details: {
          input: [
            {
              data: [[1, 2, 3]],
              context: { launchMissleCode: 1234 },
            },
          ],
          status: 'completed',
          output: {
            data: [
              {
                data: [[1, 2, 3]],
                context: { launchMissleCode: 1234 },
              },
            ],
            context: { launchMissleCode: 1234 },
          },
        },
      },
    })
  })
  test('completed', async () => {
    const { start, getSnapshot } = runTopology(spec, dag)
    await start()
    expect(getSnapshot()).toMatchObject({
      status: 'completed',
      data: {
        api: {
          deps: [],
          status: 'completed',
          input: [],
          output: [1, 2, 3],
        },
        details: {
          deps: ['api'],
          status: 'completed',
          input: [[1, 2, 3]],
          output: {
            '1': 'description 1',
            '2': 'description 2',
            '3': 'description 3',
          },
        },
        attachments: {
          deps: ['api'],
          status: 'completed',
          input: [[1, 2, 3]],
          output: { '1': 'file1.jpg', '2': 'file2.jpg', '3': 'file3.jpg' },
        },
        writeToDB: {
          deps: ['details', 'attachments'],
          status: 'completed',
          input: [
            {
              '1': 'description 1',
              '2': 'description 2',
              '3': 'description 3',
            },
            { '1': 'file1.jpg', '2': 'file2.jpg', '3': 'file3.jpg' },
          ],
          output: null,
        },
      },
    })
  })
  test('gracefully shutdown when stop is called', async () => {
    const spec: Spec = {
      api: {
        deps: [],
        run: async ({ signal, updateState }) => {
          for (let i = 0; i < 5; i++) {
            if (signal.aborted) {
              throw new Error('Aborted')
            }
            await timers.setTimeout(100)
            updateState({ index: i })
          }
        },
      },
    }

    const { start, stop, getSnapshot } = runTopology(spec, dag)
    setTimeout(stop, 200)
    await expect(start()).rejects.toThrow('Errored nodes: ["api"]')
    // Node errored
    expect(getSnapshot()).toMatchObject({
      status: 'errored',
      data: {
        api: {
          input: [],
          status: 'errored',
          state: { index: 1 },
          error: { stack: expect.stringContaining('Error: Aborted') },
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
      data: {
        api: {
          deps: [],
          started: new Date('2020-01-01T00:00:00Z'),
          finished: new Date('2020-01-01T00:00:01Z'),
          input: [],
          status: 'completed',
          output: [1, 2, 3],
        },
        details: {
          deps: ['api'],
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
          deps: ['api'],
          started: new Date('2020-01-01T00:00:00Z'),
          input: [[1, 2, 3]],
          status: 'errored',
          error: 'fail',
          state: 0,
        },
      },
    }
    const snapshot = getResumeSnapshot(errorSnapshot)
    // Started is set to current time
    expect(snapshot.started.getTime()).toBeGreaterThan(
      errorSnapshot.started.getTime()
    )
    // Finished should be removed
    expect(snapshot.finished).toBeUndefined()
    expect(snapshot).toMatchObject({
      status: 'running',
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
  const modifiedSpec = _.set('attachments.run', attachmentsRun, spec)

  test('resume after initial error', async () => {
    const { start, getSnapshot } = runTopology(modifiedSpec, dag)
    await expect(start()).rejects.toThrow('Errored nodes: ["attachments"]')
    const snapshot = getSnapshot()
    expect(snapshot).toMatchObject({
      status: 'errored',
      data: {
        api: {
          deps: [],
          input: [],
          status: 'completed',
          output: [1, 2, 3],
        },
        details: {
          deps: ['api'],
          input: [[1, 2, 3]],
          status: 'completed',
          output: {
            '1': 'description 1',
            '2': 'description 2',
            '3': 'description 3',
          },
        },
        attachments: {
          deps: ['api'],
          input: [[1, 2, 3]],
          status: 'errored',
          error: {
            stack: expect.stringContaining('Error: Failed processing id: 2'),
          },
          state: {
            index: 0,
            output: {
              '1': 'file1.jpg',
            },
          },
        },
      },
    })
    const { start: start2, getSnapshot: getSnapshot2 } = await resumeTopology(
      modifiedSpec,
      snapshot
    )
    await start2()
    expect(getSnapshot2()).toMatchObject({
      status: 'completed',
      data: {
        api: {
          deps: [],
          input: [],
          status: 'completed',
          output: [1, 2, 3],
        },
        details: {
          deps: ['api'],
          input: [[1, 2, 3]],
          status: 'completed',
          output: {
            1: 'description 1',
            2: 'description 2',
            3: 'description 3',
          },
        },
        attachments: {
          deps: ['api'],
          input: [[1, 2, 3]],
          status: 'completed',
          output: {
            1: 'file1.jpg',
            2: 'file2.jpg',
            3: 'file3.jpg',
          },
        },
        writeToDB: {
          deps: ['details', 'attachments'],
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
  test('resuming completed snapshot should be idempotent', async () => {
    const snapshot: Snapshot = {
      started: new Date('2022-01-01T12:00:00Z'),
      finished: new Date('2022-01-01T12:00:01Z'),
      status: 'completed',
      data: {
        api: {
          deps: [],
          input: [1, 2, 3],
          status: 'completed',
          output: {
            data: [1, 2, 3],
          },
        },
        details: {
          deps: ['api'],
          input: [
            {
              data: [1, 2, 3],
            },
          ],
          status: 'completed',
          output: {
            data: [
              {
                data: [1, 2, 3],
              },
            ],
          },
        },
      },
    }
    const { start, getSnapshot } = resumeTopology(spec, snapshot)
    await start()
    expect(getSnapshot()).toEqual(snapshot)
  })
  test('should throw if snapshot is undefined', async () => {
    const snapshot = undefined
    expect(() => {
      resumeTopology(spec, snapshot)
    }).toThrow(new TopologyError('Snapshot is undefined'))
  })
})
