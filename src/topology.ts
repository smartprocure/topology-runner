import _ from 'lodash/fp'
import {
  ObjectOfPromises,
  DAG,
  Events,
  Snapshot,
  Options,
  SnapshotData,
  RunTopologyInternal,
  ResumeTopology,
  RunTopology,
  Spec,
  NodeType,
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
 * Get the dependents of the node.
 */
const getDependents = (node: string, dag: DAG) => {
  const nodes: string[] = []
  for (const key in dag) {
    const { deps } = dag[key]
    if (deps.includes(node)) {
      nodes.push(key)
    }
  }
  return nodes
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
 * Get the input data from dependencies.
 */
export const getInputData = (dag: DAG, node: string, data: SnapshotData) => {
  // Use existing input data from snapshot if it exists
  const snapshotInput = _.get([node, 'input'], data)
  if (snapshotInput) {
    return snapshotInput
  }
  const deps = dag[node].deps
  const input = []
  for (const dep of deps) {
    const type = dag[dep].type
    // Input is the output of the dependency
    if (type === 'work') {
      input.push(_.get([dep, 'output'], data))
    }
    // Input is the input of the dependency
    else {
      const depInput = _.get([dep, 'input'], data)
      if (depInput) {
        input.push(...depInput)
      }
    }
  }
  return input
}

/**
 * Update the snapshot when various node-level events take place
 * and emit the modified snapshot.
 */
const nodeEventHandler =
  (snapshot: Snapshot, emitter: EventEmitter<Events>) => (node: string) => {
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
    const completed = (output?: any) => {
      // Update snapshot
      if (output !== undefined) {
        snapshot.data[node].output = output
      }
      snapshot.data[node].status = 'completed'
      snapshot.data[node].finished = new Date()
      // Emit
      emitter.emit('data', snapshot)
    }
    const branched = (selected: string, reason?: string) => {
      // Update snapshot
      snapshot.data[node].status = 'completed'
      snapshot.data[node].selected = selected
      if (reason) {
        snapshot.data[node].reason = reason
      }
      snapshot.data[node].finished = new Date()
      // Emit
      emitter.emit('data', snapshot)
    }
    const skipped = () => {
      // Update snapshot
      snapshot.data[node].status = 'skipped'
      snapshot.data[node].finished = new Date()
      // Emit
      emitter.emit('data', snapshot)
    }
    const suspended = () => {
      // Update snapshot
      snapshot.data[node].status = 'suspended'
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
    return {
      updateState,
      running,
      completed,
      branched,
      skipped,
      suspended,
      errored,
    }
  }

/**
 * Check if all nodes in the DAG are in the spec
 */
export const getMissingSpecNodes = (spec: Spec, dag: DAG) =>
  _.difference(Object.keys(dag), Object.keys(spec))

const branch = (node: string, reason?: string) => ({
  node,
  reason,
})

const NONE = Symbol('NONE')

const none = (reason?: string) => ({
  node: NONE,
  reason,
})

// eslint-disable-next-line
const noOp = async () => {}

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
  const eventHandler = nodeEventHandler(snapshot, emitter)

  const start = async () => {
    // Emit initial snapshot
    emitter.emit('data', snapshot)
    // Loop
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
        // Was part of the toplogy suspended?
        const suspended = findKeys({ status: 'suspended' }, snapshot.data)
        // Update snapshot
        const status = hasErrors
          ? 'errored'
          : suspended.length
          ? 'suspended'
          : 'completed'
        snapshot.status = status
        // Get nodes with pending status
        const pending = findKeys({ status: 'pending' }, snapshot.data)
        // Suspend pending nodes if topology is suspended
        if (status === 'suspended') {
          pending.forEach((node) => eventHandler(node).suspended())
        }
        // Skip pending nodes if topology is completed
        if (status === 'completed') {
          pending.forEach((node) => eventHandler(node).skipped())
        }
        snapshot.finished = new Date()
        // Emit
        emitter.emit(hasErrors ? 'error' : 'done', snapshot)
        // Throw an exception, causing the promise to reject, if one or more
        // nodes have errored
        if (hasErrors) {
          throw new TopologyError(`Errored nodes: ${JSON.stringify(errored)}`)
        }
        // Exit loop
        return
      }

      // Run nodes that have not been run yet
      for (const node of readyToRunNodes) {
        // Snapshot updater
        const events = eventHandler(node)
        // Get the node from the spec
        const { run, type } = spec[node]
        // Get the input data from dependencies
        const data = getInputData(dag, node, snapshot.data)
        // Run fn input
        const baseInput = { data, node, context }
        // Update snapshot
        events.running(data)
        // Get dependent nodes
        const dependents = getDependents(node, dag)

        // Handle branching node type
        if (type === 'branching') {
          const runFn = async () => run({ ...baseInput, branch, none })
          promises[node] = runFn()
            .then(({ node: selected, reason }) => {
              events.branched(selected.toString(), reason)
              // Explicitly skip all dependents
              if (selected === NONE) {
                // Skip all dependents
                dependents.forEach((node) => eventHandler(node).skipped())
              } else if (typeof selected === 'string') {
                // Check if selected branch is a dependent of the node
                if (!dependents.includes(selected)) {
                  throw new TopologyError(`Branch not found: ${selected}`)
                }
                // Skip dependents that were not selected
                _.pull(selected, dependents).forEach((node) =>
                  eventHandler(node).skipped()
                )
              }
            })
            .catch(events.errored)
            .finally(() => {
              delete promises[node]
            })
        }
        // Handle suspension and work node types
        else {
          // Callback to update state
          const updateState = events.updateState
          // Get node state. Will only be present if resuming.
          const state = snapshot.data[node]?.state
          const input = {
            ...baseInput,
            updateState,
            state,
            signal: abortController.signal,
          }
          const handleSuspension = () => {
            // Set status to completed
            events.completed()
            // Suspend all dependent nodes
            dependents.forEach((node) => eventHandler(node).suspended())
          }
          // The run fn may not exist for suspension node type
          const runFn = run || noOp
          promises[node] = runFn(input)
            .then(type === 'suspension' ? handleSuspension : events.completed)
            .catch(events.errored)
            .finally(() => {
              delete promises[node]
            })
        }
      }

      // Wait for a promise to resolve
      await Promise.race(_.values(promises))
    }
  }

  const getSnapshot = () => snapshot
  const stop = () => {
    abortController.abort()
  }

  return { start, stop, emitter, getSnapshot }
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
    // Data exists and node has no dependencies
    if (data !== undefined && noDepsNodes.includes(node)) {
      snapshotData[node].input = [data]
    }
  }
  return snapshotData
}

const extractDag = (
  obj: Record<string, { deps: string[]; type?: NodeType }>
) => {
  const dag: DAG = {}

  for (const node in obj) {
    const nodeDef = obj[node]
    // Default node type to work if not set
    dag[node] = { deps: nodeDef.deps, type: nodeDef.type || 'work' }
  }

  return dag
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
  const _dag = extractDag(spec)
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
 * NodeDef fields. Ignore skipped nodes.
 */
const resetUncompletedNodes = (data: SnapshotData): SnapshotData =>
  _.mapValues((nodeData) => {
    const { status, ...obj } = nodeData
    return status === 'completed' || status === 'skipped'
      ? nodeData
      : {
          ..._.pick(['input', 'state', 'deps', 'type'], obj),
          status: 'pending',
        }
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
    return { start: () => Promise.resolve(), stop, emitter, getSnapshot }
  }
  // Initialize snapshot for running
  const snap = getResumeSnapshot(snapshot)
  const dag = extractDag(snap.data)
  // Run the topology
  return _runTopology(spec, dag, snap, options?.context)
}
