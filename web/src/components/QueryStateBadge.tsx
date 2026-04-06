import { Badge } from '@/components/ui/badge'
import { cn } from '@/lib/utils'

const stateStyles: Record<string, string> = {
  Running: 'bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-300',
  Queued: 'bg-yellow-100 text-yellow-800 dark:bg-yellow-900 dark:text-yellow-300',
  Planning: 'bg-yellow-100 text-yellow-800 dark:bg-yellow-900 dark:text-yellow-300',
  Starting: 'bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-300',
  Finishing: 'bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-300',
  Finished: 'bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-300',
  Failed: 'bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-300',
  Cancelled: 'bg-gray-100 text-gray-800 dark:bg-gray-800 dark:text-gray-300',
}

export function QueryStateBadge({ state }: { state: string }) {
  return (
    <Badge
      variant="outline"
      className={cn('text-xs font-medium', stateStyles[state])}
    >
      {state}
    </Badge>
  )
}
