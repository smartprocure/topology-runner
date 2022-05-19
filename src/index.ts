import _ from 'lodash/fp'
import {
  ObjectOfPromises,
  DAG,
  Events,
  Snapshot,
  Options,
  Topology,
  SnapshotData,
} from './types'
import { missingKeys, findKeys, raceObject } from './util'
import EventEmitter from 'eventemitter3'

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
const filterDAG = (dag: DAG, options: Options = {}): DAG => {
  if (options.excludeNodes) return removeExcludeNodes(options.excludeNodes)(dag)
  if (options.includeNodes) return pickIncludeNodes(options.includeNodes)(dag)
  return dag
}

/**
 * Get a list of nodes where all dependencies have completed.
 * Excludes nodes that have already completed or are running.
 */
const getNodesReadyToRun = (dag: DAG, data: SnapshotData) => {
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
const initResources = async (topology: Topology, resources: string[]) => {
  const readyResources: any[] = await Promise.all(
    resources.map((resource) =>
      _.result(['resources', resource, 'init'], topology)
    )
  )
  return _.zipObj(resources, readyResources)
}

/**
 * Calculate resources that are needed but not initialized.
 * Mutates initialized, adding the missing resources.
 */
const initMissingResources = async (
  topology: Topology,
  resources: string[],
  initialized: Record<string, any>
) => {
  const missing = missingKeys(resources, initialized)
  if (missing) {
    const newInit = await initResources(topology, missing)
    for (const key in newInit) {
      const val = newInit[key]
      initialized[key] = val
    }
  }
}

/**
 * Generate an object of named inputs from the outputs of the node's
 * dependencies.
 */
const getInputData = (dag: DAG, snapshot: Snapshot, node: string) => {
  // Node deps
  const deps = dag[node].deps
  return _.flow(_.pick(deps), _.mapValues('output'))(snapshot.data)
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
export const runTopology = (topology: Topology, options: Options = {}) => {
  // Get the filtered dag
  const dag = filterDAG(topology.dag, options)
  const nodes = Object.keys(dag)
  // Initialized resources
  const initialized: Record<string, any> = {}
  // Track node promises
  const promises: ObjectOfPromises = {}
  // Initial snapshot
  const snapshot: Snapshot = { status: 'running', dag, data: {} }
  const emitter = new EventEmitter<Events>()
  let allDone = false
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
        // Emit
        emitter.emit('done', snapshot)
        // We're done
        return resolve(snapshot)
      }
      // Get nodes with resolved dependencies that have not been run
      const readyToRunNodes = getNodesReadyToRun(dag, snapshot.data)
      // Run nodes
      for (const node of readyToRunNodes) {
        const updateStateFn = (state: any) => {
          // Update snapshot
          snapshot.data[node].state = state
          // Emit
          emitter.emit('data', snapshot)
        }
        // Get the node
        const { run, resources = [] } = topology.nodes[node]
        // Initialize resources for node if needed
        await initMissingResources(topology, resources, initialized)
        // Use initial data if node has no dependencies, otherwise, data from
        // completed nodes
        const data =
          dag[node].deps.length === 0
            ? options.data
            : getInputData(dag, snapshot, node)
        // Get the subset of resources needed for the node
        const pickedResources = _.pick(resources, initialized)
        // Run fn input
        const runInput = { data, resources: pickedResources, updateStateFn }
        // Preparing to call run fn
        snapshot.data[node].started = new Date()
        snapshot.data[node].input = data
        snapshot.data[node].status = 'running'
        // Call run fn
        promises[node] = run(runInput)
          .then((output) => {
            // Update snapshot
            snapshot.data[node].output = output
            snapshot.data[node].status = 'completed'
            snapshot.data[node].finished = new Date()
            // Emit
            emitter.emit('data', snapshot)
          })
          .catch((e) => {
            // Update snapshot
            snapshot.data[node].status = 'errored'
            snapshot.data[node].finished = new Date()
            snapshot.status = 'errored'
            snapshot.error = e
            // Emit
            emitter.emit('error', snapshot)
            // We're done
            allDone = true
            reject(e)
          })
      }
      // Wait for a promise to resolve
      const node = await raceObject(promises)
      // We run this code after awaiting to allow for the promise.catch
      // callback to execute.
      if (allDone) {
        return
      }
      // Don't track the resolved promise anymore
      delete promises[node]
    }
  })
  return { emitter, promise }
}

export const resumeTopology = (topology: Topology, snapshot: Snapshot) => {

}
