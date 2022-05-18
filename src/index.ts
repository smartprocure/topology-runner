import _ from 'lodash/fp'
import {
  ObjectOfPromises,
  DAG,
  Snapshot,
  Options,
  Topology,
  SnapshotData,
} from './types'
import { missingKeys, findKeys, raceObject } from './util'
import EventEmitter from 'node:events'

const removeExcludeNodes = (excludeNodes: string[]) =>
  _.flow(
    _.omit(excludeNodes),
    _.mapValues(_.update('deps', _.pullAll(excludeNodes)))
  )

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
  const runningOrCompleted = Object.keys(data)
  const nodes: string[] = []
  for (const node in dag) {
    const { deps } = dag[node]
    if (_.difference(deps, completed).length === 0) {
      nodes.push(node)
    }
  }
  // Exclude nodes that have already completed or are running
  return _.difference(nodes, runningOrCompleted)
}

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
 * Mutates initialized adding the missing resources.
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
 */
const runTopology = (topology: Topology, options: Options = {}) => {
  // Get the filtered dag
  const dag = filterDAG(topology.dag, options)
  const nodes = Object.keys(dag)
  // Initialized resources
  const initialized: Record<string, any> = {}
  const id = topology.id
  // Initial snapshot
  const snapshot: Snapshot = { id, status: 'running', dag, data: {} }
  const emitter = new EventEmitter()
  // Emit
  emitter.emit('data', snapshot)

  const promise = new Promise<Snapshot>(async (resolve, reject) => {
    // Track node promises
    const promises: ObjectOfPromises = {}
    while (true) {
      const completed = findKeys({ status: 'completed' }, snapshot.data)
      // All nodes have completed
      if (completed.length === nodes.length) {
        // Update snapshot
        snapshot.status = 'completed'
        // Emit
        emitter.emit('done', snapshot)
        // We're done
        resolve(snapshot)
      }
      // Get nodes with resolved dependencies that have not been run
      const readyToRunNodes = getNodesReadyToRun(dag, snapshot.data)
      // Run nodes
      for (const node of readyToRunNodes) {
        const updateStateFn = (state: any) => {
          snapshot.data[node].state = state
        }
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
            snapshot.data[node].completed = new Date()
            // Don't track promise anymore
            delete promises[node]
            // Emit
            emitter.emit('data', snapshot)
          })
          .catch((e) => {
            // Update snapshot
            snapshot.status = 'errored'
            snapshot.error = e
            // Emit
            emitter.emit('error', snapshot)
            // We're done
            reject(e)
          })
      }
      // Wait for a promise to resolve
      await raceObject(promises)
    }
  })
  return { emitter, promise }
}

export default runTopology
