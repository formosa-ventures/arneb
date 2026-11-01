import { LogOut } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { ThemeToggle } from '@/components/ThemeToggle'
import { MobileSidebar } from './Sidebar'

export function Header({ onSignOut }: { onSignOut?: () => void }) {
  return (
    <header className="flex h-14 items-center gap-4 border-b px-4 lg:px-6">
      <MobileSidebar />
      <div className="flex-1" />
      {onSignOut && (
        <Button variant="ghost" size="sm" onClick={onSignOut}>
          <LogOut />
          Sign out
        </Button>
      )}
      <ThemeToggle />
    </header>
  )
}
