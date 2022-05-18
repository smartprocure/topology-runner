interface Resource<A> {
  init(): Promise<A>
}

type ResourceInitializers = Record<string, Resource<any>>
type UpdateStateFn = (state: any) => void

interface RunInput {
  resources: Record<string, any>
  data: any
  updateStateFn: UpdateStateFn
}

interface NodeDef {
  run(arg: RunInput): Promise<any>
  resources?: string[]
}
export type DAG = Record<string, { deps: string[] }>

export interface Topology {
  id: string
  resources?: ResourceInitializers
  nodes: Record<string, NodeDef>
  dag: DAG
}

export interface Options {
  includeNodes?: string[]
  excludeNodes?: string[]
  data?: any // Fed into starting nodes (i.e., nodes with no dependencies)
}

export type Status = 'running' | 'completed' | 'errored'

interface NodeData {
  status: Status
  started: Date
  completed: Date
  input: any
  output: any
  state: any
}

export type SnapshotData = Record<string, NodeData>

export interface Snapshot {
  id: string
  status: Status
  dag: DAG
  data: SnapshotData
  error?: any
}

export type ObjectOfPromises = Record<string, Promise<any>>
