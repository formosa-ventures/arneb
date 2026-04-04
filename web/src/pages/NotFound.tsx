import { Link } from 'react-router-dom'
import { Card, CardContent } from '@/components/ui/card'

export function NotFound() {
  return (
    <div className="flex items-center justify-center min-h-[60vh]">
      <Card className="max-w-md w-full">
        <CardContent className="py-12 text-center space-y-4">
          <h1 className="text-4xl font-bold">404</h1>
          <p className="text-muted-foreground">Page not found</p>
          <Link
            to="/"
            className="text-sm text-primary hover:underline inline-block"
          >
            Back to Dashboard
          </Link>
        </CardContent>
      </Card>
    </div>
  )
}
