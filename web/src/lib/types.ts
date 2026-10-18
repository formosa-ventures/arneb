export interface QueryResponse {
  query_id: string
  state: string
  sql: string
  error: string | null
}

export interface QueriesResponse {
  queries: QueryResponse[]
}

export interface ClusterResponse {
  worker_count: number
  role: string
  /** Admission counters; max_running_queries 0 means unlimited. */
  running_queries: number
  queued_queries: number
  max_running_queries: number
  max_queued_queries: number
}

export interface WorkerResponse {
  worker_id: string
  address: string
  alive: boolean
  max_splits: number
  last_heartbeat_secs_ago: number
}

export interface InfoResponse {
  version: string
  uptime_secs: number
  role: string
}
