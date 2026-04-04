import type {
  QueriesResponse,
  QueryResponse,
  ClusterResponse,
  WorkerResponse,
  InfoResponse,
} from './types'

const BASE = '/api/v1'

async function fetchJson<T>(url: string, init?: RequestInit): Promise<T> {
  const res = await fetch(url, init)
  if (!res.ok) {
    const body = await res.text().catch(() => '')
    throw new Error(`HTTP ${res.status}: ${body}`)
  }
  return res.json()
}

export async function getQueries(state?: string): Promise<QueriesResponse> {
  const params = state ? `?state=${encodeURIComponent(state)}` : ''
  return fetchJson<QueriesResponse>(`${BASE}/queries${params}`)
}

export async function getQuery(id: string): Promise<QueryResponse> {
  return fetchJson<QueryResponse>(`${BASE}/queries/${encodeURIComponent(id)}`)
}

export async function cancelQuery(id: string): Promise<void> {
  const res = await fetch(`${BASE}/queries/${encodeURIComponent(id)}`, {
    method: 'DELETE',
  })
  if (!res.ok) {
    const body = await res.text().catch(() => '')
    throw new Error(`HTTP ${res.status}: ${body}`)
  }
}

export async function getCluster(): Promise<ClusterResponse> {
  return fetchJson<ClusterResponse>(`${BASE}/cluster`)
}

export async function getWorkers(): Promise<WorkerResponse[]> {
  return fetchJson<WorkerResponse[]>(`${BASE}/cluster/workers`)
}

export async function getInfo(): Promise<InfoResponse> {
  return fetchJson<InfoResponse>(`${BASE}/info`)
}
