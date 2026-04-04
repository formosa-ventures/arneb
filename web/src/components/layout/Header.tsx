import { ThemeToggle } from '@/components/ThemeToggle'
import { MobileSidebar } from './Sidebar'

export function Header() {
  return (
    <header className="flex h-14 items-center gap-4 border-b px-4 lg:px-6">
      <MobileSidebar />
      <div className="flex-1" />
      <ThemeToggle />
    </header>
  )
}
