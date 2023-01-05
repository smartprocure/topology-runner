import EventEmitter from 'eventemitter3'

type UpdateState = (state: any) => void
export type Initialized = Record<string, any>

export interface BaseInput {
  data: any
  node: string
  context?: any
}

// Work
export interface WorkInput extends BaseInput {
  updateState: UpdateState
  state?: any
  signal: AbortSignal
}

export type WorkOutput = Promise<any>

export type RunFn = (arg: WorkInput) => WorkOutput

export interface WorkNodeDef {
  run: RunFn
  deps: string[]
  type?: 'work'
}

// Branching
export interface BranchingInput extends BaseInput {
  branch: (node: string, reason?: string) => BranchingOutput
  none: (reason?: string) => BranchingOutput
}

export interface BranchingOutput {
  node: string | symbol
  reason?: string
}

export interface BranchingNodeDef {
  run: (arg: BranchingInput) => BranchingOutput
  deps: string[]
  type: 'branching'
}

// Suspension
export type SuspensionInput = WorkInput
export type SuspensionOutput = Promise<void>

export interface SuspensionNodeDef {
  run?: (arg: SuspensionInput) => SuspensionOutput
  deps: string[]
  type: 'suspension'
}

export type NodeType = 'work' | 'branching' | 'suspension'
export type Input = BranchingInput | SuspensionInput | WorkInput
export type Output = BranchingOutput | SuspensionOutput | WorkOutput
export type NodeDef = BranchingNodeDef | SuspensionNodeDef | WorkNodeDef

export type DAG = Record<string, { deps: string[], type: NodeType }>
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

export type Status =
  | 'pending'
  | 'running'
  | 'completed'
  | 'suspended'
  | 'errored'

export type NodeStatus = Status | 'skipped'

export interface NodeData {
  type: NodeType,
  deps: string[]
  status: NodeStatus
  started?: Date
  finished?: Date
  input?: any
  output?: any
  state?: any
  error?: any
  reason?: string
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
