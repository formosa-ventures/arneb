import { useCallback } from 'react'
import { Server, Wifi, WifiOff } from 'lucide-react'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { Separator } from '@/components/ui/separator'
import { useAutoRefresh } from '@/hooks/useAutoRefresh'
import { getInfo, getWorkers } from '@/lib/api'
import { formatDuration, cn } from '@/lib/utils'
import type { InfoResponse, WorkerResponse } from '@/lib/types'

interface ClusterData {
  info: InfoResponse
  workers: WorkerResponse[]
}

export function Cluster() {
  const fetchAll = useCallback(async (): Promise<ClusterData> => {
    const [info, workers] = await Promise.all([getInfo(), getWorkers()])
    return { info, workers }
  }, [])

  const { data, isLoading } = useAutoRefresh(fetchAll)

  if (isLoading && !data) {
    return <div className="text-muted-foreground">Loading...</div>
  }

  const info = data?.info
  const workers = data?.workers ?? []
  const alive = workers.filter((w) => w.alive)
  const dead = workers.filter((w) => !w.alive)
  const totalSplits = alive.reduce((sum, w) => sum + w.max_splits, 0)
  const sortedWorkers = [...alive, ...dead].sort((a, b) =>
    a.alive === b.alive ? a.worker_id.localeCompare(b.worker_id) : a.alive ? -1 : 1
  )

  return (
    <div className="space-y-6">
      <h1 className="text-2xl font-bold">Cluster</h1>

      {info && (
        <Card>
          <CardHeader>
            <CardTitle className="text-sm font-medium">Server Info</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="flex flex-wrap gap-6 text-sm">
              <div>
                <span className="text-muted-foreground">Version</span>
                <p className="font-medium">{info.version}</p>
              </div>
              <div>
                <span className="text-muted-foreground">Uptime</span>
                <p className="font-medium">{formatDuration(info.uptime_secs)}</p>
              </div>
              <div>
                <span className="text-muted-foreground">Role</span>
                <p>
                  <Badge variant="outline" className="capitalize">
                    {info.role}
                  </Badge>
                </p>
              </div>
            </div>
          </CardContent>
        </Card>
      )}

      {info?.role === 'standalone' ? (
        <Card>
          <CardContent className="py-8 text-center">
            <Server className="h-8 w-8 mx-auto text-muted-foreground mb-2" />
            <p className="text-muted-foreground">
              Running in standalone mode. No workers to display.
            </p>
          </CardContent>
        </Card>
      ) : (
        <>
          <Card>
            <CardContent className="py-3">
              <div className="flex flex-wrap gap-4 text-sm">
                <span>
                  <strong>{workers.length}</strong> total
                </span>
                <Separator orientation="vertical" className="h-5" />
                <span className="text-green-600 dark:text-green-400">
                  <strong>{alive.length}</strong> healthy
                </span>
                <Separator orientation="vertical" className="h-5" />
                <span className="text-red-600 dark:text-red-400">
                  <strong>{dead.length}</strong> unhealthy
                </span>
                <Separator orientation="vertical" className="h-5" />
                <span>
                  <strong>{totalSplits}</strong> total splits
                </span>
              </div>
            </CardContent>
          </Card>

          {workers.length === 0 ? (
            <Card>
              <CardContent className="py-8 text-center">
                <p className="text-muted-foreground">No workers connected</p>
              </CardContent>
            </Card>
          ) : (
            <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
              {sortedWorkers.map((w) => (
                <Card key={w.worker_id}>
                  <CardHeader className="flex flex-row items-center gap-3 pb-2">
                    <span
                      className={cn(
                        'h-2.5 w-2.5 rounded-full shrink-0',
                        w.alive ? 'bg-green-500' : 'bg-red-500'
                      )}
                    />
                    <CardTitle className="text-sm font-medium">{w.worker_id}</CardTitle>
                  </CardHeader>
                  <CardContent className="space-y-1 text-sm">
                    <div className="flex items-center gap-2 text-muted-foreground">
                      {w.alive ? (
                        <Wifi className="h-3.5 w-3.5" />
                      ) : (
                        <WifiOff className="h-3.5 w-3.5" />
                      )}
                      <span className="font-mono text-xs">{w.address}</span>
                    </div>
                    <p className="text-muted-foreground">
                      {w.max_splits} splits
                    </p>
                    <p className="text-xs text-muted-foreground">
                      {w.alive
                        ? `Last seen: ${w.last_heartbeat_secs_ago}s ago`
                        : `Last seen: ${formatDuration(w.last_heartbeat_secs_ago)} ago`}
                    </p>
                  </CardContent>
                </Card>
              ))}
            </div>
          )}
        </>
      )}
    </div>
  )
}
