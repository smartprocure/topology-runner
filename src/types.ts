import EventEmitter from 'eventemitter3'

interface Resource<A> {
  init(): Promise<A> | A
  cleanup?(resource: A): Promise<void> | void
}

type ResourceInitializers = Record<string, Resource<any>>
type UpdateStateFn = (state: any) => void
export type Initialized = Record<string, any>

export interface RunInput {
  resources: Record<string, any>
  data: any
  updateStateFn: UpdateStateFn
  state?: any
  signal: AbortSignal
}

type Millis = number
export type RunFn = (arg: RunInput) => Promise<any>

interface NodeDef {
  run: RunFn
  resources?: string[]
  timeout?: Millis
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

export type Response = {
  emitter: EventEmitter<Events,any>,
  promise: Promise<Snapshot>,
  getSnapshot: () => Snapshot
}
export type Status = 'pending' | 'running' | 'completed' | 'errored'

export interface NodeData {
  status: Status
  started?: Date
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

export type ObjectOfPromises = Record<string | number, Promise<any>>

export type Events = 'data' | 'error' | 'done'
