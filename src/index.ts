import _ from 'lodash/fp'
import {
  ObjectOfPromises,
  DAG,
  TopologySnapshot,
  Options,
  Topology,
} from './types'
import { raceObject } from './util'
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

const filterDAG = (dag: DAG, options: Options = {}): DAG => {
  if (options.excludeNodes) return removeExcludeNodes(options.excludeNodes)(dag)
  if (options.includeNodes) return pickIncludeNodes(options.includeNodes)(dag)
  return dag
}

const getNodesReadyToRun = (dag: DAG, completed: string[]) => {
  const nodes: string[] = []
  for (const node in dag) {
    const { deps } = dag[node]
    if (_.difference(deps, completed).length === 0) {
      nodes.push(node)
    }
  }
  return _.difference(nodes, completed)
}

const initResources = async (
  topology: Topology,
  initialized: object,
  resources: string[]
) => {
  if (_.isEmpty(topology.resources)) return initialized
  const missing = _.flow(Object.keys, _.difference(resources))(initialized)
  if (missing) {
    const readyResources = await Promise.all(
      missing.map((resource) =>
        _.result(['resources', resource, 'init'], topology)
      )
    )
    const resourceObj = _.zipObj(missing, readyResources)
    return _.merge(initialized, resourceObj)
  }
  return initialized
}

const getInputData = (dag: DAG, snapshot: TopologySnapshot, node: string) => {
  const deps = dag[node].deps
  return _.flow(_.pick(deps), _.mapValues('output'))(snapshot.data)
}

const runTopology = (topology: Topology, options: Options = {}) => {
  const dag = filterDAG(topology.dag, options)
  const nodes = Object.keys(dag)
  const completed: string[] = []
  let initialized = {}
  const snapshot: TopologySnapshot = {
    id: topology.id,
    status: 'running',
    dag,
    data: {},
  }
  const emitter = new EventEmitter()
  emitter.emit('data', snapshot)

  const promise = new Promise<TopologySnapshot>(async (resolve, reject) => {
    const promises: ObjectOfPromises = {}
    while (true) {
      if (completed.length === nodes.length) {
        const snap: TopologySnapshot = { ...snapshot, status: 'completed' }
        emitter.emit('done', snap)
        resolve(snap)
      }
      // Get nodes with resolved dependencies that have not been run
      const readyToRunNodes = getNodesReadyToRun(dag, completed)
      for (const node of readyToRunNodes) {
        const updateStateFn = (state: any) => {
          snapshot.data[node].state = state
        }
        const { run, resources = [] } = topology.nodes[node]
        // Initialize resources for node if needed
        initialized = await initResources(topology, initialized, resources)
        // Use initial data if node has no dependencies, otherwise, data from
        // completed nodes
        const data =
          dag[node].deps.length === 0
            ? options.data
            : getInputData(dag, snapshot, node)
        // Get the subset of resources needed for the node
        const pickedResources = _.pick(resources, initialized)
        const runInput = { data, resources: pickedResources, updateStateFn }
        snapshot.data[node].started = new Date()
        promises[node] = run(runInput).then((output) => {
          completed.push(node)
          snapshot.data[node].output = output
          snapshot.data[node].status = 'completed'
          snapshot.data[node].completed = new Date()
        })
      }
      try {
        const key = await raceObject(promises)
        delete promises[key]
        emitter.emit('data', snapshot)
      } catch (e) {
        const snap: TopologySnapshot = {
          ...snapshot,
          status: 'errored',
          error: e,
        }
        emitter.emit('error', snap)
        reject(e)
      }
    }
  })
  return { emitter, promise }
}
