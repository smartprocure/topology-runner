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
  Initialized,
  RunTopologyInternal,
  ResumeTopology,
  RunTopology,
} from './types'
import { missingKeys, findKeys } from './util'
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
    // The status is not set for the node on the snapshot or status is pending
    const isPending = !data[node]?.status || data[node]?.status === 'pending'
    // Dependencies for the node are all completed and the node is currently
    // pending.
    if (_.difference(deps, completed).length === 0 && isPending) {
      nodes.push(node)
    }
  }
  return nodes
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
    delete snapshot.data[node].error
    // Emit
    emitter.emit('data', snapshot)
  }
  const errored = (error: any) => {
    // Update snapshot
    snapshot.data[node].status = 'errored'
    snapshot.data[node].finished = new Date()
    snapshot.data[node].error = error.toString()
    // Emit
    emitter.emit('data', snapshot)
  }
  return { updateState, running, completed, errored }
}

/**
 * Check if all nodes in the DAG are in the spec
 */
export const getMissingSpecNodes = (spec: Spec, dag: DAG) =>
  _.difference(Object.keys(dag), Object.keys(spec.nodes))

const _runTopology: RunTopologyInternal = (spec, snapshot, dag, context) => {
  const missingSpecNodes = getMissingSpecNodes(spec, dag)
  if (missingSpecNodes.length) {
    throw new TopologyError(
      `Missing the following nodes in spec: ${missingSpecNodes.join(', ')}`
    )
  }
  // Initialized resources
  const initialized: Initialized = {}
  // Track node promises
  const promises: ObjectOfPromises = {}
  // Event emitter
  const emitter = new EventEmitter<Events>()
  // Emit initial snapshot
  emitter.emit('data', snapshot)

  const run = async () => {
    while (true) {
      // Get nodes with resolved dependencies that have not been run
      const readyToRunNodes = getNodesReadyToRun(dag, snapshot.data)

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
        // Cleanup initialized resources
        await cleanupResources(spec, initialized)
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
        const { run, resources = [] } = spec.nodes[node]
        // Initialize resources for node if needed
        await initMissingResources(spec, resources, initialized)
        // Use initial data if node has no dependencies, otherwise, data from
        // completed nodes
        const data = getInputData(dag, node, snapshot.data)
        // Get the subset of resources required for the node
        const reqResources = _.pick(resources, initialized)
        // Callback to update state
        const updateState = events.updateState
        // Get node state. Will only be present if resuming.
        const state = snapshot.data[node]?.state
        // Run fn input
        const runInput: RunInput = {
          data,
          resources: reqResources,
          updateState,
          state,
          context,
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

  return { emitter, promise: run(), getSnapshot }
}

/**
 * Set input for nodes with no dependencies to data, if exists.
 */
export const initSnapshotData = (dag: DAG, data?: any) => {
  if (_.isEmpty(data)) {
    return {}
  }
  // Get nodes with no dependencies
  const noDepsNodes = findKeys(
    ({ deps }: { deps: string[] }) => _.isEmpty(deps),
    dag
  )
  // Initialize data
  return noDepsNodes.reduce(
    (acc, node) => _.set([node, 'input'], data, acc),
    {}
  )
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
export const runTopology: RunTopology = (spec, inputDag, options) => {
  // Get the filtered dag
  const dag = filterDAG(inputDag, options)
  // Initialize snapshot data
  const data = initSnapshotData(dag, options?.data)
  // Initial snapshot
  const snapshot: Snapshot = {
    status: 'running',
    started: new Date(),
    dag,
    data,
  }
  // Run the topology
  return _runTopology(spec, snapshot, dag, options?.context)
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
      : { ..._.pick(['input', 'state'], obj), status: 'pending' }
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
  // Ensures resumption is idempotent
  if (snapshot.status === 'completed') {
    const emitter = new EventEmitter<Events>()
    const getSnapshot = () => snapshot
    return { emitter, promise: Promise.resolve(), getSnapshot }
  }
  // Initialize snapshot for running
  const snap = getResumeSnapshot(snapshot)
  // Run the topology
  return _runTopology(spec, snap, snap.dag, options?.context)
}
