<script setup lang="ts">
const repo = 'formosa-ventures/arneb'

const badges = [
  {
    alt: 'Latest release',
    src: `https://img.shields.io/github/v/release/${repo}?sort=semver`,
    href: `https://github.com/${repo}/releases/latest`
  },
  {
    alt: 'CI',
    src: `https://github.com/${repo}/actions/workflows/ci.yml/badge.svg`,
    href: `https://github.com/${repo}/actions/workflows/ci.yml`
  },
  {
    alt: 'Dependency audit',
    src: `https://img.shields.io/github/actions/workflow/status/${repo}/deny.yml?branch=main&label=cargo-deny`,
    href: `https://github.com/${repo}/actions/workflows/deny.yml`
  },
  {
    alt: 'License: Apache-2.0',
    src: 'https://img.shields.io/badge/license-Apache--2.0-blue',
    href: `https://github.com/${repo}#license`
  },
  {
    alt: 'Rust 1.89+',
    src: 'https://img.shields.io/badge/rust-1.89%2B-orange?logo=rust',
    href: '/guide/quickstart'
  },
  {
    alt: 'TPC-H 22/22 cell-identical to Trino',
    src: 'https://img.shields.io/badge/TPC--H-22%2F22%20vs%20Trino-brightgreen',
    href: '/guide/performance'
  },
  {
    alt: 'SF10: 2.3x faster, 0.14x memory vs Trino',
    src: 'https://img.shields.io/badge/SF10%20vs%20Trino-2.3%C3%97%20faster%20%C2%B7%200.14%C3%97%20memory-brightgreen',
    href: '/guide/performance'
  },
  {
    alt: 'Last commit',
    src: `https://img.shields.io/github/last-commit/${repo}`,
    href: `https://github.com/${repo}/commits/main`
  }
]
</script>

<template>
  <div class="home-badges">
    <a v-for="b in badges" :key="b.alt" :href="b.href">
      <img :src="b.src" :alt="b.alt" height="20" loading="lazy" />
    </a>
  </div>
</template>
