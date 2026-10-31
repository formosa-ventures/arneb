import type {
  QueriesResponse,
  QueryResponse,
  ClusterResponse,
  WorkerResponse,
  InfoResponse,
} from './types'

const BASE = '/api/v1'
const SESSION_KEY = 'arneb.session'

/** Window event fired when the server rejects the stored session. */
export const UNAUTHENTICATED_EVENT = 'arneb:unauthenticated'

export function getSessionToken(): string | null {
  try {
    return sessionStorage.getItem(SESSION_KEY)
  } catch {
    return null
  }
}

export function setSessionToken(token: string): void {
  try {
    sessionStorage.setItem(SESSION_KEY, token)
  } catch {
    // Storage unavailable (private mode, blocked): the token is not kept.
  }
}

export function clearSessionToken(): void {
  try {
    sessionStorage.removeItem(SESSION_KEY)
  } catch {
    // Nothing to clear.
  }
}

/** Merge the session's Authorization header into request options. */
function withAuth(init?: RequestInit): RequestInit {
  const token = getSessionToken()
  if (!token) return init ?? {}
  const headers = new Headers(init?.headers)
  headers.set('Authorization', `Bearer ${token}`)
  return { ...init, headers }
}

/** Fetch with the session token; a 401 `unauthenticated` signs the UI out. */
async function authedFetch(url: string, init?: RequestInit): Promise<Response> {
  const res = await fetch(url, withAuth(init))
  if (res.status === 401) {
    const code = await res
      .clone()
      .json()
      .then((b) => b?.error?.code)
      .catch(() => undefined)
    if (code === 'unauthenticated') {
      clearSessionToken()
      window.dispatchEvent(new Event(UNAUTHENTICATED_EVENT))
    }
  }
  return res
}

async function fetchJson<T>(url: string, init?: RequestInit): Promise<T> {
  const res = await authedFetch(url, init)
  if (!res.ok) {
    const body = await res.text().catch(() => '')
    throw new Error(`HTTP ${res.status}: ${body}`)
  }
  return res.json()
}

export interface HealthResponse {
  status: string
  version: string
  auth: 'none' | 'password'
}

export async function getHealth(): Promise<HealthResponse> {
  const res = await fetch('/api/v2/health')
  if (!res.ok) throw new Error(`HTTP ${res.status}`)
  return res.json()
}

/** Sign in; stores the session token. Throws an Error with a readable message. */
export async function signIn(user: string, password: string): Promise<void> {
  const res = await fetch('/api/v2/sessions', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ user, password }),
  })
  if (!res.ok) {
    const body = await res.json().catch(() => undefined)
    throw new Error(body?.error?.message ?? `Sign-in failed (HTTP ${res.status})`)
  }
  const { token } = await res.json()
  setSessionToken(token)
}

/** Revoke the session server-side (best effort) and forget the token. */
export async function signOut(): Promise<void> {
  const token = getSessionToken()
  clearSessionToken()
  if (!token) return
  try {
    await fetch('/api/v2/sessions/current', {
      method: 'DELETE',
      headers: { Authorization: `Bearer ${token}` },
    })
  } catch {
    // The token is already dropped locally; it expires on its own.
  }
}

export async function getQueries(state?: string): Promise<QueriesResponse> {
  const params = state ? `?state=${encodeURIComponent(state)}` : ''
  return fetchJson<QueriesResponse>(`${BASE}/queries${params}`)
}

export async function getQuery(id: string): Promise<QueryResponse> {
  return fetchJson<QueryResponse>(`${BASE}/queries/${encodeURIComponent(id)}`)
}

export async function cancelQuery(id: string): Promise<void> {
  const res = await authedFetch(`${BASE}/queries/${encodeURIComponent(id)}`, {
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
