import { defineConfig } from 'vitepress'

export default defineConfig({
  title: 'Arneb',
  description: 'A distributed SQL query engine built in Rust',
  base: '/arneb/',

  themeConfig: {
    nav: [
      { text: 'Guide', link: '/guide/introduction' },
      { text: 'SQL Reference', link: '/sql/overview' },
      { text: 'Connectors', link: '/connectors/overview' },
      { text: 'Architecture', link: '/architecture/overview' }
    ],

    sidebar: {
      '/guide/': [
        {
          text: 'Guide',
          items: [
            { text: 'Introduction', link: '/guide/introduction' },
            { text: 'Quickstart', link: '/guide/quickstart' },
            { text: 'Configuration', link: '/guide/configuration' },
            { text: 'Distributed Mode', link: '/guide/distributed' }
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
            { text: 'Hive', link: '/connectors/hive' }
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
