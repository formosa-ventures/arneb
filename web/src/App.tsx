import { Routes, Route } from 'react-router-dom'
import { Layout } from '@/components/layout/Layout'
import { Dashboard } from '@/pages/Dashboard'
import { Queries } from '@/pages/Queries'
import { QueryDetail } from '@/pages/QueryDetail'
import { Cluster } from '@/pages/Cluster'
import { NotFound } from '@/pages/NotFound'

export default function App() {
  return (
    <Routes>
      <Route element={<Layout />}>
        <Route index element={<Dashboard />} />
        <Route path="queries" element={<Queries />} />
        <Route path="queries/:id" element={<QueryDetail />} />
        <Route path="cluster" element={<Cluster />} />
        <Route path="*" element={<NotFound />} />
      </Route>
    </Routes>
  )
}
