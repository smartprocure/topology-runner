import EventEmitter from 'eventemitter3'

interface Resource<A> {
  init(): Promise<A> | A
  cleanup?(resource: A): Promise<void> | void
}

type ResourceInitializers = Record<string, Resource<any>>
type UpdateState = (state: any) => void
export type Initialized = Record<string, any>

export interface RunInput {
  resources: Record<string, any>
  data: any
  updateState: UpdateState
  state?: any
  context?: any
  node: string
  signal: AbortSignal
}

export type RunFn = (arg: RunInput) => Promise<any>

interface NodeDef {
  run: RunFn
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
  context?: any // Fed to all nodes
}

export type Response = {
  emitter: EventEmitter<Events, any>
  promise: Promise<void>
  getSnapshot(): Snapshot
  stop(): void
}
export type Status = 'pending' | 'running' | 'completed' | 'errored'

export interface NodeData {
  status?: Status
  started?: Date
  finished?: Date
  input: any
  output?: any
  state?: any
  error?: any
}

export type SnapshotData = Record<string, NodeData>

export interface Snapshot {
  status: Status
  started: Date
  finished?: Date
  dag: DAG
  data: SnapshotData
  context?: any
}

export type ObjectOfPromises = Record<string | number, Promise<any>>

export type Events = 'data' | 'error' | 'done'

export type RunTopology = (
  spec: Spec,
  inputDag: DAG,
  options?: Options
) => Response

export type RunTopologyInternal = (
  spec: Spec,
  snapshot: Snapshot,
  dag: DAG,
  context: any
) => Response

export type ResumeTopology = (
  spec: Spec,
  snapshot?: Snapshot,
  options?: Pick<Options, 'context'>
) => Response
