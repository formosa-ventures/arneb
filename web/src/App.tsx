import { useCallback, useEffect, useState } from 'react'
import { Routes, Route } from 'react-router-dom'
import { Layout } from '@/components/layout/Layout'
import { Dashboard } from '@/pages/Dashboard'
import { Login } from '@/pages/Login'
import { Queries } from '@/pages/Queries'
import { QueryDetail } from '@/pages/QueryDetail'
import { Cluster } from '@/pages/Cluster'
import { NotFound } from '@/pages/NotFound'
import {
  UNAUTHENTICATED_EVENT,
  getHealth,
  getSessionToken,
  signOut,
} from '@/lib/api'

export default function App() {
  // null until /api/v2/health answers; a failed probe falls back to "none"
  // so the UI still renders (API calls will surface their own errors).
  const [auth, setAuth] = useState<'none' | 'password' | null>(null)
  const [signedIn, setSignedIn] = useState(() => getSessionToken() !== null)

  useEffect(() => {
    getHealth()
      .then((h) => setAuth(h.auth))
      .catch(() => setAuth('none'))
  }, [])

  useEffect(() => {
    const onUnauth = () => setSignedIn(false)
    window.addEventListener(UNAUTHENTICATED_EVENT, onUnauth)
    return () => window.removeEventListener(UNAUTHENTICATED_EVENT, onUnauth)
  }, [])

  const handleSignOut = useCallback(async () => {
    await signOut()
    setSignedIn(false)
  }, [])

  if (auth === null) return null
  if (auth === 'password' && !signedIn) {
    return <Login onSignedIn={() => setSignedIn(true)} />
  }

  return (
    <Routes>
      <Route
        element={<Layout onSignOut={auth === 'password' ? handleSignOut : undefined} />}
      >
        <Route index element={<Dashboard />} />
        <Route path="queries" element={<Queries />} />
        <Route path="queries/:id" element={<QueryDetail />} />
        <Route path="cluster" element={<Cluster />} />
        <Route path="*" element={<NotFound />} />
      </Route>
    </Routes>
  )
}
