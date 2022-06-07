import _ from 'lodash/fp'
import {
  ObjectOfPromises,
  DAG,
  Events,
  Snapshot,
  Options,
  RunInput,
  Spec,
  SnapshotData,
  Response,
  Initialized,
} from './types'
import { missingKeys, findKeys, raceObject } from './util'
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
 * Get a list of nodes where all dependencies have completed.
 * Excludes nodes that have already completed or are running.
 */
export const getNodesReadyToRun = (dag: DAG, data: SnapshotData) => {
  const completed = findKeys({ status: 'completed' }, data)
  const running = findKeys({ status: 'running' }, data)
  const nodes: string[] = []
  for (const node in dag) {
    const { deps } = dag[node]
    if (_.difference(deps, completed).length === 0) {
      nodes.push(node)
    }
  }
  // Exclude nodes that have already completed or are running
  return _.difference(nodes, [...completed, ...running])
}

/**
 * Call init on resources.
 */
export const initResources = async (spec: Spec, resources: string[]) => {
  const values = await Promise.all(
    resources.map((resource) => spec?.resources?.[resource]?.init())
  )
  return _.zipObj(resources, values)
}

/**
 * Call cleanup on resources.
 */
export const cleanupResources = (spec: Spec, initialized: Initialized) => {
  const ps = []
  for (const [key, val] of Object.entries(initialized)) {
    const cleanupFn = spec?.resources?.[key]?.cleanup
    if (cleanupFn) {
      ps.push(cleanupFn(val))
    }
  }
  return Promise.all(ps)
}

/**
 * Calculate resources that are needed but not initialized.
 * Mutates initialized, adding the missing resources.
 */
const initMissingResources = async (
  spec: Spec,
  resources: string[],
  initialized: Initialized
) => {
  const missing = missingKeys(resources, initialized)
  if (missing) {
    const newInit = await initResources(spec, missing)
    for (const key in newInit) {
      const val = newInit[key]
      initialized[key] = val
    }
  }
}

/**
 * Generate an array from the outputs of the node's dependencies.
 */
export const getInputData = (dag: DAG, node: string, data: SnapshotData) => {
  // Get input data off of snapshot if exists
  if (_.has([node, 'input'], data)) {
    return _.get([node, 'input'], data)
  }
  // Node deps
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
    snapshot.data[node] = {
      started: new Date(),
      input: data,
      status: 'running',
    }
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
    const date = new Date()
    // Update snapshot
    snapshot.data[node].status = 'errored'
    snapshot.data[node].finished = date
    snapshot.status = 'errored'
    snapshot.error = error instanceof Error ? error.message : error
    snapshot.finished = date
    // Emit
    emitter.emit('error', snapshot)
  }
  return { updateState, running, completed, errored }
}

/**
 * Check if all nodes in the DAG are in the spec
 */
export const getMissingSpecNodes = (spec: Spec, dag: DAG) =>
  _.difference(Object.keys(dag), Object.keys(spec.nodes))

const _runTopology = (spec: Spec, snapshot: Snapshot, dag: DAG): Response => {
  const missingSpecNodes = getMissingSpecNodes(spec, dag)
  if (missingSpecNodes.length) {
    throw new TopologyError(
      `Missing the following nodes in spec: ${missingSpecNodes.join(', ')}`
    )
  }
  const nodes = Object.keys(dag)
  // Initialized resources
  const initialized: Initialized = {}
  // Track node promises
  const promises: ObjectOfPromises = {}
  const emitter = new EventEmitter<Events>()
  let done = false
  // Emit
  emitter.emit('data', snapshot)

  const promise = new Promise<Snapshot>(async (resolve, reject) => {
    while (true) {
      // Get completed nodes
      const completed = findKeys({ status: 'completed' }, snapshot.data)
      // All nodes have completed
      if (completed.length === nodes.length) {
        // Update snapshot
        snapshot.status = 'completed'
        snapshot.finished = new Date()
        // Emit
        emitter.emit('done', snapshot)
        // Cleanup initialized resources
        await cleanupResources(spec, initialized)
        // We're done
        return resolve(snapshot)
      }
      // Get nodes with resolved dependencies that have not been run
      const readyToRunNodes = getNodesReadyToRun(dag, snapshot.data)
      // Run nodes
      for (const node of readyToRunNodes) {
        // Snapshot updater
        const events = nodeEventHandler(node, snapshot, emitter)
        // Get the node
        const { run, resources = [], timeout } = spec.nodes[node]
        // Initialize resources for node if needed
        await initMissingResources(spec, resources, initialized)
        // Use initial data if node has no dependencies, otherwise, data from
        // completed nodes
        const data = getInputData(dag, node, snapshot.data)
        // Get the subset of resources required for the node
        const reqResources = _.pick(resources, initialized)
        // Callback to update state
        const updateState = events.updateState
        // Resume scenario
        const state = snapshot.data[node]?.state
        // Abort after timeout
        const abortController = new AbortController()
        if (timeout) {
          setTimeout(
            () => abortController.abort(`Timeout occurred after ${timeout} ms`),
            timeout
          )
        }
        // Run fn input
        const runInput: RunInput = {
          data,
          resources: reqResources,
          updateState,
          state,
          signal: abortController.signal,
          meta: snapshot?.meta
        }
        // Update snapshot
        events.running(data)
        // Call run fn
        promises[node] = run(runInput)
          .then(events.completed)
          .catch((e) => {
            events.errored(e)
            // We're done
            done = true
            reject(e)
          })
      }
      // Wait for a promise to resolve
      const node = await raceObject(promises)
      // We run this code after awaiting to allow for the promise.catch
      // callback to execute.
      if (done) {
        // Cleanup initialized resources
        await cleanupResources(spec, initialized)
        return
      }
      // Don't track the resolved promise anymore
      delete promises[node]
    }
  })
  const getSnapshot = () => snapshot

  return { emitter, promise, getSnapshot }
}

/*
 * Set input for nodes with no dependencies to options.data,
 * if exists.
 */
export const initSnapshotData = (dag: DAG, options: Options = {}) => {
  if (!_.has('data', options)) {
    return {}
  }
  // Get nodes with no dependencies
  const noDepsNodes = findKeys(
    ({ deps }: { deps: string[] }) => _.isEmpty(deps),
    dag
  )
  // Initialize data
  return noDepsNodes.reduce(
    (acc, node) => _.set([node, 'input'], options.data, acc),
    {}
  )
}

/**
 * Run a topology consisting of a DAG and functions for each node in the
 * DAG. A subset of the DAG can be executed by setting either includeNodes
 * or excludeNodes. Initial data is passed via options.data.
 *
 * Returns an event emitter and a promise. The event emitter emits data
 * every time the topology snapshot updates, done when the topology completes,
 * and error when a node throws an error.
 */
export const runTopology = (spec: Spec, inputDag: DAG, options?: Options) => {
  // Get the filtered dag
  const dag = filterDAG(inputDag, options)
  // Initialize snapshot data
  const data = initSnapshotData(dag, options)
  // Initial snapshot
  const snapshot: Snapshot = {
    status: 'running',
    started: new Date(),
    dag,
    data,
    meta: options?.meta
  }
  // Run the topology
  return _runTopology(spec, snapshot, dag)
}

/**
 * Set uncompleted nodes to pending and reset appropriate
 * NodeDef fields.
 */
const resetUncompletedNodes = (data: SnapshotData): SnapshotData =>
  _.mapValues((nodeData) => {
    const { status, ...obj } = nodeData
    return status === 'completed'
      ? nodeData
      : { ..._.pick(['input', 'state'], obj), status: 'pending' }
  }, data)

export const getResumeSnapshot = (snapshot: Snapshot) => {
  const snap: Snapshot = {
    ...snapshot,
    status: 'running',
    started: new Date(),
    data: resetUncompletedNodes(snapshot.data),
  }
  delete snap.error
  delete snap.finished
  return snap
}
/**
 * Resume a topology from a previous snapshot.
 */
export const resumeTopology = (spec: Spec, snapshot: Snapshot): Response => {
  // Ensures resumption is idempotent
  if (snapshot.status === 'completed') {
    const emitter = new EventEmitter<Events>()
    const getSnapshot = () => snapshot
    return { emitter, promise: Promise.resolve(snapshot), getSnapshot }
  }
  // Initialize snapshot for running
  const snap = getResumeSnapshot(snapshot)
  // Run the topology
  return _runTopology(spec, snap, snap.dag)
}
