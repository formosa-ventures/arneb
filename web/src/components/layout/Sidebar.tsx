import { NavLink } from 'react-router-dom'
import { LayoutDashboard, Search, Server, Menu } from 'lucide-react'
import { cn } from '@/lib/utils'
import { Sheet, SheetContent, SheetTrigger } from '@/components/ui/sheet'
import { Separator } from '@/components/ui/separator'
import { useState } from 'react'

const navItems = [
  { to: '/', label: 'Dashboard', icon: LayoutDashboard },
  { to: '/queries', label: 'Queries', icon: Search },
  { to: '/cluster', label: 'Cluster', icon: Server },
] as const

function NavItems({ onClick }: { onClick?: () => void }) {
  return (
    <nav className="flex flex-col gap-1 px-2">
      {navItems.map(({ to, label, icon: Icon }) => (
        <NavLink
          key={to}
          to={to}
          end={to === '/'}
          onClick={onClick}
          className={({ isActive }) =>
            cn(
              'flex items-center gap-3 rounded-lg px-3 py-2 text-sm font-medium transition-colors',
              isActive
                ? 'bg-accent text-accent-foreground'
                : 'text-muted-foreground hover:bg-accent hover:text-accent-foreground'
            )
          }
        >
          <Icon className="h-4 w-4 shrink-0" />
          <span className="hidden lg:inline">{label}</span>
        </NavLink>
      ))}
    </nav>
  )
}

export function Sidebar() {
  return (
    <aside className="hidden md:flex md:w-14 lg:w-56 flex-col border-r border-sidebar-border bg-sidebar shrink-0">
      <div className="flex h-14 items-center px-4 lg:px-6">
        <span className="hidden lg:inline text-lg font-semibold text-sidebar-foreground">
          Arneb
        </span>
      </div>
      <Separator />
      <div className="flex-1 py-4">
        <NavItems />
      </div>
    </aside>
  )
}

export function MobileSidebar() {
  const [open, setOpen] = useState(false)

  return (
    <Sheet open={open} onOpenChange={setOpen}>
      <SheetTrigger className="inline-flex items-center justify-center rounded-md p-2 text-muted-foreground hover:bg-accent hover:text-accent-foreground md:hidden">
        <Menu className="h-5 w-5" />
        <span className="sr-only">Toggle menu</span>
      </SheetTrigger>
      <SheetContent side="left" className="w-56 p-0">
        <div className="flex h-14 items-center px-6">
          <span className="text-lg font-semibold">Arneb</span>
        </div>
        <Separator />
        <div className="py-4">
          <NavItems onClick={() => setOpen(false)} />
        </div>
      </SheetContent>
    </Sheet>
  )
}
