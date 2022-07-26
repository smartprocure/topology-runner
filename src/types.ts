import EventEmitter from 'eventemitter3'

type UpdateState = (state: any) => void
export type Initialized = Record<string, any>

export interface RunInput {
  data: any
  updateState: UpdateState
  state?: any
  context?: any
  node: string
  signal: AbortSignal
}

export type RunFn = (arg: RunInput) => Promise<any>

export interface NodeDef {
  run: RunFn
  deps: string[]
}

export type DAG = Record<string, { deps: string[] }>
export type Spec = Record<string, NodeDef>

export interface Options {
  includeNodes?: string[]
  excludeNodes?: string[]
  data?: any // Fed into starting nodes (i.e., nodes with no dependencies)
  context?: any // Fed to all nodes
}

export type Response = {
  start(): Promise<void>
  stop(): void
  emitter: EventEmitter<Events, any>
  getSnapshot(): Snapshot
}
export type Status = 'pending' | 'running' | 'completed' | 'errored' | 'aborted'

export interface NodeData {
  deps: string[]
  status: Status
  started?: Date
  finished?: Date
  input?: any
  output?: any
  state?: any
  error?: any
}

export type SnapshotData = Record<string, NodeData>

export interface Snapshot {
  status: Status
  started: Date
  finished?: Date
  data: SnapshotData
}

export type ObjectOfPromises = Record<string | number, Promise<any>>

export type Events = 'data' | 'error' | 'done'

export type RunTopology = (spec: Spec, options?: Options) => Response

export type RunTopologyInternal = (
  spec: Spec,
  dag: DAG,
  snapshot: Snapshot,
  context: any
) => Response

export type ResumeTopology = (
  spec: Spec,
  snapshot?: Snapshot,
  options?: Pick<Options, 'context'>
) => Response
