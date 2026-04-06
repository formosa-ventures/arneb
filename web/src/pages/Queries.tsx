import { useCallback, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { QueryStateBadge } from '@/components/QueryStateBadge'
import { useAutoRefresh } from '@/hooks/useAutoRefresh'
import { getQueries, cancelQuery } from '@/lib/api'
import { truncate } from '@/lib/utils'

const STATES = ['All', 'Running', 'Queued', 'Finished', 'Failed', 'Cancelled'] as const

export function Queries() {
  const navigate = useNavigate()
  const [stateFilter, setStateFilter] = useState('All')

  const fetchQueries = useCallback(
    () => getQueries(stateFilter === 'All' ? undefined : stateFilter),
    [stateFilter]
  )

  const { data, refresh } = useAutoRefresh(fetchQueries)

  const queries = data?.queries ?? []

  const handleCancel = async (e: React.MouseEvent, queryId: string) => {
    e.stopPropagation()
    await cancelQuery(queryId)
    refresh()
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-bold">Queries</h1>
        <Select value={stateFilter} onValueChange={(v) => v && setStateFilter(v)}>
          <SelectTrigger className="w-[150px]">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            {STATES.map((s) => (
              <SelectItem key={s} value={s}>
                {s}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>

      <Card>
        <CardHeader>
          <CardTitle className="text-sm font-medium">
            {stateFilter === 'All' ? 'All Queries' : `${stateFilter} Queries`}
          </CardTitle>
        </CardHeader>
        <CardContent>
          {queries.length === 0 ? (
            <p className="text-sm text-muted-foreground">No queries match this filter</p>
          ) : (
            <div className="overflow-x-auto">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>ID</TableHead>
                    <TableHead>SQL</TableHead>
                    <TableHead>State</TableHead>
                    <TableHead className="w-[100px]">Action</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {queries.map((q) => (
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
                      <TableCell>
                        {(q.state === 'Running' || q.state === 'Queued') && (
                          <Button
                            variant="destructive"
                            size="sm"
                            onClick={(e) => handleCancel(e, q.query_id)}
                          >
                            Cancel
                          </Button>
                        )}
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
