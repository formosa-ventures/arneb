import { h } from 'vue'
import DefaultTheme from 'vitepress/theme'
import type { Theme } from 'vitepress'
import GitHubStar from './components/GitHubStar.vue'
import HomeBadges from './components/HomeBadges.vue'
import './custom.css'

export default {
  extends: DefaultTheme,
  Layout: () =>
    h(DefaultTheme.Layout, null, {
      // Every page: a star button in the top nav bar.
      'nav-bar-content-after': () => h(GitHubStar, { class: 'nav-star' }),
      // Home: a large star button above the hero title, badges below the actions.
      'home-hero-info-before': () => h(GitHubStar, { size: 'large', class: 'hero-star' }),
      'home-hero-actions-after': () => h(HomeBadges)
    })
} satisfies Theme
