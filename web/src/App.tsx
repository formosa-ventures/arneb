import { useCallback, useEffect, useState } from 'react'
import { Routes, Route } from 'react-router-dom'
import { Button } from '@/components/ui/button'
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

type AuthMode = 'none' | 'password'

const PROBE_ATTEMPTS = 4

/** Read the auth mode, retrying with a short backoff; null if unreachable. */
async function probeAuth(): Promise<AuthMode | null> {
  for (let i = 0; i < PROBE_ATTEMPTS; i++) {
    try {
      return (await getHealth()).auth
    } catch {
      await new Promise((r) => setTimeout(r, 300 * 2 ** i))
    }
  }
  return null
}

export default function App() {
  // `auth` is null until a probe succeeds; a failed probe is never assumed
  // to mean "none" (a password server would then 401 with no way to sign in).
  const [auth, setAuth] = useState<AuthMode | null>(null)
  const [probing, setProbing] = useState(true)
  const [signedIn, setSignedIn] = useState(() => getSessionToken() !== null)

  const probe = useCallback(async () => {
    setProbing(true)
    setAuth(await probeAuth())
    setProbing(false)
  }, [])

  useEffect(() => {
    // eslint-disable-next-line react-hooks/set-state-in-effect
    void probe()
  }, [probe])

  useEffect(() => {
    const onUnauth = () => {
      setSignedIn(false)
      // A 401 while we think auth is off means the mode was never learned.
      if (auth !== 'password') void probe()
    }
    window.addEventListener(UNAUTHENTICATED_EVENT, onUnauth)
    return () => window.removeEventListener(UNAUTHENTICATED_EVENT, onUnauth)
  }, [auth, probe])

  const handleSignOut = useCallback(async () => {
    await signOut()
    setSignedIn(false)
  }, [])

  if (auth === null) {
    if (probing) {
      return <div className="p-6 text-muted-foreground">Loading...</div>
    }
    return (
      <div className="flex min-h-screen flex-col items-center justify-center gap-3 p-4">
        <p className="text-muted-foreground">Cannot reach the server.</p>
        <Button onClick={() => void probe()}>Retry</Button>
      </div>
    )
  }
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
