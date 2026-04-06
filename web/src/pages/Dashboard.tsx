import { useCallback } from 'react'
import { useNavigate } from 'react-router-dom'
import { Activity, CheckCircle2, XCircle, Server } from 'lucide-react'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'
import { QueryStateBadge } from '@/components/QueryStateBadge'
import { useAutoRefresh } from '@/hooks/useAutoRefresh'
import { getQueries, getCluster, getWorkers } from '@/lib/api'
import { truncate } from '@/lib/utils'
import type { QueriesResponse, ClusterResponse, WorkerResponse } from '@/lib/types'

interface DashboardData {
  queries: QueriesResponse
  cluster: ClusterResponse
  workers: WorkerResponse[]
}

export function Dashboard() {
  const navigate = useNavigate()

  const fetchAll = useCallback(async (): Promise<DashboardData> => {
    const [queries, cluster, workers] = await Promise.all([
      getQueries(),
      getCluster(),
      getWorkers(),
    ])
    return { queries, cluster, workers }
  }, [])

  const { data, isLoading } = useAutoRefresh(fetchAll)

  if (isLoading && !data) {
    return <div className="text-muted-foreground">Loading...</div>
  }

  const queries = data?.queries.queries ?? []
  const cluster = data?.cluster
  const workers = data?.workers ?? []

  const running = queries.filter((q) => q.state === 'Running').length
  const finished = queries.filter((q) => q.state === 'Finished').length
  const failed = queries.filter((q) => q.state === 'Failed').length
  const aliveWorkers = workers.filter((w) => w.alive).length

  const recentQueries = queries.slice(0, 10)

  return (
    <div className="space-y-6">
      <h1 className="text-2xl font-bold">Dashboard</h1>

      <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
        <Card>
          <CardHeader className="flex flex-row items-center justify-between pb-2">
            <CardTitle className="text-sm font-medium">Running</CardTitle>
            <Activity className="h-4 w-4 text-blue-500" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">{running}</div>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between pb-2">
            <CardTitle className="text-sm font-medium">Completed</CardTitle>
            <CheckCircle2 className="h-4 w-4 text-green-500" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">{finished}</div>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between pb-2">
            <CardTitle className="text-sm font-medium">Failed</CardTitle>
            <XCircle className="h-4 w-4 text-red-500" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">{failed}</div>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between pb-2">
            <CardTitle className="text-sm font-medium">Workers</CardTitle>
            <Server className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">
              {cluster?.role === 'standalone' ? 'Standalone' : aliveWorkers}
            </div>
          </CardContent>
        </Card>
      </div>

      {cluster && cluster.role !== 'standalone' && (
        <Card>
          <CardHeader>
            <CardTitle className="text-sm font-medium">Cluster</CardTitle>
          </CardHeader>
          <CardContent>
            <p className="text-sm text-muted-foreground">
              <span className="capitalize font-medium text-foreground">{cluster.role}</span>
              {' — '}
              {workers.length} workers ({aliveWorkers} healthy)
            </p>
          </CardContent>
        </Card>
      )}

      <Card>
        <CardHeader>
          <CardTitle className="text-sm font-medium">Recent Queries</CardTitle>
        </CardHeader>
        <CardContent>
          {recentQueries.length === 0 ? (
            <p className="text-sm text-muted-foreground">No queries yet</p>
          ) : (
            <div className="overflow-x-auto">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>ID</TableHead>
                    <TableHead>SQL</TableHead>
                    <TableHead>State</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {recentQueries.map((q) => (
                    <TableRow
                      key={q.query_id}
                      className="cursor-pointer"
                      onClick={() => navigate(`/queries/${q.query_id}`)}
                    >
                      <TableCell className="font-mono text-xs">
                        {q.query_id.slice(0, 8)}
                      </TableCell>
                      <TableCell className="max-w-md">
                        <span className="text-sm">{truncate(q.sql, 80)}</span>
                      </TableCell>
                      <TableCell>
                        <QueryStateBadge state={q.state} />
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  )
}
