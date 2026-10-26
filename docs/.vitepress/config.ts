import { defineConfig } from 'vitepress'

// https://vitepress.dev/reference/site-config
export default defineConfig({
  title: 'Arneb',
  description: 'A distributed SQL query engine built in Rust',
  base: '/',
  // Internal planning docs, not part of the site.
  srcExclude: ['superpowers/**'],

  themeConfig: {
    nav: [
      { text: 'Guide', link: '/guide/introduction' },
      { text: 'SQL Reference', link: '/sql/overview' },
      { text: 'Connectors', link: '/connectors/overview' },
      { text: 'Architecture', link: '/architecture/overview' },
      { text: 'Performance', link: '/guide/performance' },
      {
        text: 'v0.1.6', // x-release-please-version
        items: [
          { text: 'Release notes', link: 'https://github.com/formosa-ventures/arneb/releases' },
          { text: 'Changelog', link: 'https://github.com/formosa-ventures/arneb/blob/main/CHANGELOG.md' },
          { text: 'Contributing', link: 'https://github.com/formosa-ventures/arneb/blob/main/docs/contributing.md' }
        ]
      }
    ],

    sidebar: {
      '/guide/': [
        {
          text: 'Guide',
          items: [
            { text: 'Introduction', link: '/guide/introduction' },
            { text: 'Quickstart', link: '/guide/quickstart' },
            { text: 'Configuration', link: '/guide/configuration' },
            { text: 'Distributed Mode', link: '/guide/distributed' },
            { text: 'Kubernetes', link: '/guide/kubernetes' },
            { text: 'Access Control', link: '/guide/access-control' },
            { text: 'Observability', link: '/guide/observability' },
            { text: 'Performance', link: '/guide/performance' },
            { text: 'Trino Clients', link: '/guide/trino-clients' },
            { text: 'Client Compatibility', link: '/guide/clients' }
          ]
        }
      ],
      '/sql/': [
        {
          text: 'SQL Reference',
          items: [
            { text: 'Overview', link: '/sql/overview' },
            { text: 'Expressions', link: '/sql/expressions' },
            { text: 'Functions', link: '/sql/functions' },
            { text: 'Advanced', link: '/sql/advanced' }
          ]
        }
      ],
      '/connectors/': [
        {
          text: 'Connectors',
          items: [
            { text: 'Overview', link: '/connectors/overview' },
            { text: 'File Connector', link: '/connectors/file' },
            { text: 'Object Store', link: '/connectors/object-store' },
            { text: 'Hive', link: '/connectors/hive' },
            { text: 'Iceberg', link: '/connectors/iceberg' },
            { text: 'PostgreSQL', link: '/connectors/postgresql' }
          ]
        }
      ],
      '/architecture/': [
        {
          text: 'Architecture',
          items: [
            { text: 'Overview', link: '/architecture/overview' }
          ]
        }
      ]
    },

    socialLinks: [
      { icon: 'github', link: 'https://github.com/formosa-ventures/arneb' }
    ],

    search: {
      provider: 'local'
    }
  }
})
