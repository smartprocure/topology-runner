import _ from 'lodash/fp'
import {
  ObjectOfPromises,
  DAG,
  Events,
  Snapshot,
  Options,
  RunInput,
  SnapshotData,
  RunTopologyInternal,
  ResumeTopology,
  RunTopology,
  NodeDef,
  Spec,
  NodeData,
} from './types'
import { findKeys } from './util'
import EventEmitter from 'eventemitter3'
import makeError from 'make-error'

export const TopologyError = makeError('TopologyError')

/**
 * Remove excludesNodes from DAG and from the dependency lists
 * of nodes.
 */
const removeExcludeNodes = (excludeNodes: string[]) =>
  _.flow(
    _.omit(excludeNodes),
    _.mapValues(_.update('deps', _.pullAll(excludeNodes)))
  )

/**
 * Only include nodes from DAG in includeNodes and remove dependencies
 * not in includeNodes.
 */
const pickIncludeNodes = (includeNodes: string[]) =>
  _.flow(
    _.pick(includeNodes),
    _.mapValues(_.update('deps', _.intersection(includeNodes)))
  )

/**
 * Handle excludeNodes and includeNodes options, transforming the
 * DAG accordingly. Only one option will be handled, not both.
 */
export const filterDAG = (dag: DAG, options: Options = {}): DAG => {
  if (options.excludeNodes) return removeExcludeNodes(options.excludeNodes)(dag)
  if (options.includeNodes) return pickIncludeNodes(options.includeNodes)(dag)
  return dag
}

/**
 * Get a list of nodes where all dependencies have completed and the
 * node's status is pending.
 */
export const getNodesReadyToRun = (dag: DAG, data: SnapshotData) => {
  // Get completed nodes
  const completed = findKeys({ status: 'completed' }, data)
  const nodes: string[] = []
  for (const node in dag) {
    const { deps } = dag[node]
    // The node does not exist or status is pending
    const isPending = !data[node] || data[node].status === 'pending'
    // Dependencies for the node are all completed and the node is currently
    // pending.
    if (_.difference(deps, completed).length === 0 && isPending) {
      nodes.push(node)
    }
  }
  return nodes
}

/**
 * Generate an array from the outputs of the node's dependencies.
 */
export const getInputData = (dag: DAG, node: string, data: SnapshotData) => {
  // Get input data off of snapshot if exists
  if (_.has([node, 'input'], data)) {
    return _.get([node, 'input'], data)
  }
  // Input is the output of the node's dependencies
  const deps = dag[node].deps
  return deps.map((dep) => _.get([dep, 'output'], data))
}

/**
 * Update the snapshot when various node-level events take place
 * and emit the modified snapshot.
 */
const nodeEventHandler = (
  node: string,
  snapshot: Snapshot,
  emitter: EventEmitter<Events>
) => {
  const updateState = (state: any) => {
    // Update snapshot
    snapshot.data[node].state = state
    // Emit
    emitter.emit('data', snapshot)
  }
  const running = (data: any) => {
    // Update snapshot
    snapshot.data[node].started = new Date()
    snapshot.data[node].input = data
    snapshot.data[node].status = 'running'
    // Emit
    emitter.emit('data', snapshot)
  }
  const completed = (output: any) => {
    // Update snapshot
    snapshot.data[node].output = output
    snapshot.data[node].status = 'completed'
    snapshot.data[node].finished = new Date()
    // Emit
    emitter.emit('data', snapshot)
  }
  const errored = (error: any) => {
    // Update snapshot
    snapshot.data[node].status = 'errored'
    snapshot.data[node].finished = new Date()
    // Capture stack and any abritrary properties if instance of Error
    snapshot.data[node].error =
      error instanceof Error ? { ...error, stack: error.stack } : error
    // Emit
    emitter.emit('data', snapshot)
  }
  return { updateState, running, completed, errored }
}

/**
 * Check if all nodes in the DAG are in the spec
 */
export const getMissingSpecNodes = (spec: Spec, dag: DAG) =>
  _.difference(Object.keys(dag), Object.keys(spec))

const _runTopology: RunTopologyInternal = (spec, dag, snapshot, context) => {
  const missingSpecNodes = getMissingSpecNodes(spec, dag)
  if (missingSpecNodes.length) {
    throw new TopologyError(
      `Missing the following nodes in spec: ${missingSpecNodes.join(', ')}`
    )
  }
  const abortController = new AbortController()
  // Track node promises
  const promises: ObjectOfPromises = {}
  // Event emitter
  const emitter = new EventEmitter<Events>()
  // Emit initial snapshot
  emitter.emit('data', snapshot)

  const run = async () => {
    while (true) {
      // Get nodes with resolved dependencies that have not been run
      const readyToRunNodes = abortController.signal.aborted
        ? []
        : getNodesReadyToRun(dag, snapshot.data)

      // There is no more work to be done
      if (_.isEmpty(readyToRunNodes) && _.isEmpty(promises)) {
        // Get errored nodes
        const errored = findKeys({ status: 'errored' }, snapshot.data)
        const hasErrors = !_.isEmpty(errored)
        // Update snapshot
        snapshot.status = hasErrors ? 'errored' : 'completed'
        snapshot.finished = new Date()
        // Emit
        emitter.emit(hasErrors ? 'error' : 'done', snapshot)
        // Throw an exception, causing the promise to reject, if one or more
        // nodes have errored
        if (hasErrors) {
          throw new TopologyError(`Errored nodes: ${JSON.stringify(errored)}`)
        }
        return
      }

      // Run nodes that have not been run yet
      for (const node of readyToRunNodes) {
        // Snapshot updater
        const events = nodeEventHandler(node, snapshot, emitter)
        // Get the node from the spec
        const { run } = spec[node]
        // Use initial data if node has no dependencies, otherwise, data from
        // completed nodes
        const data = getInputData(dag, node, snapshot.data)
        // Callback to update state
        const updateState = events.updateState
        // Get node state. Will only be present if resuming.
        const state = snapshot.data[node]?.state
        // Run fn input
        const runInput: RunInput = {
          data,
          updateState,
          state,
          context,
          node,
          signal: abortController.signal,
        }
        // Update snapshot
        events.running(data)
        // Call run fn
        promises[node] = run(runInput)
          .then(events.completed)
          .catch(events.errored)
          .finally(() => {
            delete promises[node]
          })
      }

      // Wait for a promise to resolve
      await Promise.race(_.values(promises))
    }
  }

  const getSnapshot = () => snapshot
  const stop = () => {
    abortController.abort()
  }

  return { emitter, promise: run(), getSnapshot, stop }
}

const getNodesWithNoDeps = (dag: DAG) =>
  findKeys(({ deps }: { deps: string[] }) => _.isEmpty(deps), dag)

/**
 * Set input for nodes with no dependencies to data, if exists.
 */
export const initSnapshotData = (dag: DAG, data?: any): SnapshotData => {
  // Get nodes with no dependencies
  const noDepsNodes = getNodesWithNoDeps(dag)

  const snapshotData: SnapshotData = {}

  for (const node in dag) {
    snapshotData[node] = { ...dag[node], status: 'pending' }
    // Data exists and node has no dependencies:w
    if (data !== undefined && noDepsNodes.includes(node)) {
      snapshotData[node].input = [data]
    }
  }
  return snapshotData
}

/**
 * Run a topology consisting of a DAG and functions for each node in the
 * DAG. A subset of the DAG can be executed by setting either includeNodes
 * or excludeNodes. Initial data is passed via options.data. Optionally
 * pass a context blob to all nodes.

 *
 * Returns an event emitter and a promise. The event emitter emits data
 * every time the topology snapshot updates. Events include: data, error, and
 * done.
 */
export const runTopology: RunTopology = (spec, options) => {
  const _dag: DAG = _.mapValues<NodeDef, { deps: string[] }>(
    _.pick('deps'),
    spec
  )
  // Get the filtered dag
  const dag = filterDAG(_dag, options)
  // Initialize snapshot data
  const data = initSnapshotData(dag, options?.data)
  // Initial snapshot
  const snapshot: Snapshot = {
    status: 'running',
    started: new Date(),
    data,
  }
  // Run the topology
  return _runTopology(spec, dag, snapshot, options?.context)
}

/**
 * Set uncompleted nodes to 'pending' and reset appropriate
 * NodeDef fields.
 */
const resetUncompletedNodes = (data: SnapshotData): SnapshotData =>
  _.mapValues((nodeData) => {
    const { status, ...obj } = nodeData
    return status === 'completed'
      ? nodeData
      : { ..._.pick(['input', 'state', 'deps'], obj), status: 'pending' }
  }, data)

/**
 * Transform a previously run snapshot into one that is ready to
 * be run again.
 */
export const getResumeSnapshot = (snapshot: Snapshot) => {
  const snap: Snapshot = {
    ...snapshot,
    status: 'running',
    started: new Date(),
    data: resetUncompletedNodes(snapshot.data),
  }
  delete snap.finished
  return snap
}
/**
 * Resume a topology from a previous snapshot. Optionally pass a context
 * blob to all nodes.
 *
 * Returns an event emitter and a promise. The event emitter emits data
 * every time the topology snapshot updates. Events include: data, error, and
 * done.
 */
export const resumeTopology: ResumeTopology = (spec, snapshot, options) => {
  if (!snapshot) {
    throw new TopologyError('Snapshot is undefined')
  }
  // Ensures resumption is idempotent
  if (snapshot.status === 'completed') {
    const emitter = new EventEmitter<Events>()
    const getSnapshot = () => snapshot
    /* eslint-disable-next-line */
    const stop = () => {}
    return { emitter, promise: Promise.resolve(), getSnapshot, stop }
  }
  // Initialize snapshot for running
  const snap = getResumeSnapshot(snapshot)
  const dag: DAG = _.mapValues<NodeData, { deps: string[] }>(
    _.pick('deps'),
    snap.data
  )
  // Run the topology
  return _runTopology(spec, dag, snap, options?.context)
}
