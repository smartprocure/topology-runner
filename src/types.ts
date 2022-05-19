interface Resource<A> {
  init(): Promise<A> | A
}

type ResourceInitializers = Record<string, Resource<any>>
type UpdateStateFn = (state: any) => void

interface RunInput {
  resources: Record<string, any>
  data: any
  updateStateFn: UpdateStateFn
  state?: any
}

interface NodeDef {
  run(arg: RunInput): Promise<any>
  resources?: string[]
}
export type DAG = Record<string, { deps: string[] }>

export interface Spec {
  resources?: ResourceInitializers
  nodes: Record<string, NodeDef>
}

export interface Options {
  includeNodes?: string[]
  excludeNodes?: string[]
  data?: any // Fed into starting nodes (i.e., nodes with no dependencies)
}

export type Status = 'pending' | 'running' | 'completed' | 'errored'

export interface NodeData {
  status: Status
  started: Date
  finished?: Date
  input: any
  output?: any
  state?: any
}

export type SnapshotData = Record<string, NodeData>

export interface Snapshot {
  status: Status
  started: Date
  finished?: Date
  dag: DAG
  data: SnapshotData
  error?: any
}

export type ObjectOfPromises = Record<string, Promise<any>>

export type Events = 'data' | 'error' | 'done'
