import { useCallback, useEffect, useRef, useState } from 'react'

interface UseAutoRefreshResult<T> {
  data: T | null
  isLoading: boolean
  error: Error | null
  refresh: () => void
}

export function useAutoRefresh<T>(
  fetchFn: () => Promise<T>,
  intervalMs = 2000
): UseAutoRefreshResult<T> {
  const [data, setData] = useState<T | null>(null)
  const [isLoading, setIsLoading] = useState(true)
  const [error, setError] = useState<Error | null>(null)
  const fetchRef = useRef(fetchFn)
  fetchRef.current = fetchFn

  const doFetch = useCallback(async () => {
    try {
      const result = await fetchRef.current()
      setData(result)
      setError(null)
    } catch (e) {
      setError(e instanceof Error ? e : new Error(String(e)))
    } finally {
      setIsLoading(false)
    }
  }, [])

  useEffect(() => {
    doFetch()

    const id = setInterval(() => {
      if (document.visibilityState === 'visible') {
        doFetch()
      }
    }, intervalMs)

    const onVisibility = () => {
      if (document.visibilityState === 'visible') {
        doFetch()
      }
    }
    document.addEventListener('visibilitychange', onVisibility)

    return () => {
      clearInterval(id)
      document.removeEventListener('visibilitychange', onVisibility)
    }
  }, [doFetch, intervalMs])

  return { data, isLoading, error, refresh: doFetch }
}
