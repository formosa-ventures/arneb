import { useCallback, useEffect, useRef, useState } from 'react'
import { useParams, Link } from 'react-router-dom'
import { ArrowLeft, Copy, Check } from 'lucide-react'
import hljs from 'highlight.js/lib/core'
import sql from 'highlight.js/lib/languages/sql'
import 'highlight.js/styles/github-dark.min.css'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { QueryStateBadge } from '@/components/QueryStateBadge'
import { useAutoRefresh } from '@/hooks/useAutoRefresh'
import { getQuery, cancelQuery } from '@/lib/api'

hljs.registerLanguage('sql', sql)

export function QueryDetail() {
  const { id } = useParams<{ id: string }>()
  const codeRef = useRef<HTMLElement>(null)
  const [copied, setCopied] = useState(false)

  const fetchQuery = useCallback(() => getQuery(id!), [id])
  const { data: query, error, refresh } = useAutoRefresh(fetchQuery)

  useEffect(() => {
    if (codeRef.current && query?.sql) {
      codeRef.current.textContent = query.sql
      hljs.highlightElement(codeRef.current)
    }
  }, [query?.sql])

  const handleCopy = async () => {
    if (query?.sql) {
      await navigator.clipboard.writeText(query.sql)
      setCopied(true)
      setTimeout(() => setCopied(false), 2000)
    }
  }

  const handleCancel = async () => {
    if (id) {
      await cancelQuery(id)
      refresh()
    }
  }

  if (error) {
    return (
      <div className="space-y-4">
        <Link to="/queries" className="inline-flex items-center gap-1 text-sm text-muted-foreground hover:text-foreground">
          <ArrowLeft className="h-4 w-4" /> Back to queries
        </Link>
        <Card>
          <CardContent className="py-8 text-center">
            <p className="text-muted-foreground">Query not found</p>
            <Link to="/queries" className="text-sm text-primary hover:underline mt-2 inline-block">
              Return to queries list
            </Link>
          </CardContent>
        </Card>
      </div>
    )
  }

  if (!query) {
    return <div className="text-muted-foreground">Loading...</div>
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center gap-4">
        <Link to="/queries" className="inline-flex items-center gap-1 text-sm text-muted-foreground hover:text-foreground">
          <ArrowLeft className="h-4 w-4" /> Back
        </Link>
        <h1 className="text-2xl font-bold">Query Detail</h1>
      </div>

      <Card>
        <CardHeader className="flex flex-row items-center justify-between">
          <div className="space-y-1">
            <CardTitle className="text-sm font-medium">Query ID</CardTitle>
            <p className="font-mono text-xs text-muted-foreground">{query.query_id}</p>
          </div>
          <div className="flex items-center gap-2">
            <QueryStateBadge state={query.state} />
            {(query.state === 'Running' || query.state === 'Queued') && (
              <Button variant="destructive" size="sm" onClick={handleCancel}>
                Cancel
              </Button>
            )}
          </div>
        </CardHeader>
      </Card>

      {query.error && (
        <Card className="border-destructive">
          <CardHeader>
            <CardTitle className="text-sm font-medium text-destructive">Error</CardTitle>
          </CardHeader>
          <CardContent>
            <pre className="text-sm whitespace-pre-wrap font-mono text-destructive">
              {query.error}
            </pre>
          </CardContent>
        </Card>
      )}

      <Card>
        <CardHeader className="flex flex-row items-center justify-between">
          <CardTitle className="text-sm font-medium">SQL</CardTitle>
          <Button variant="ghost" size="sm" onClick={handleCopy}>
            {copied ? (
              <><Check className="h-4 w-4 mr-1" /> Copied!</>
            ) : (
              <><Copy className="h-4 w-4 mr-1" /> Copy</>
            )}
          </Button>
        </CardHeader>
        <CardContent>
          <pre className="rounded-lg bg-muted p-4 overflow-x-auto">
            <code ref={codeRef} className="language-sql text-sm">
              {query.sql}
            </code>
          </pre>
        </CardContent>
      </Card>
    </div>
  )
}
