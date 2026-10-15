<script setup lang="ts">
import { computed } from 'vue'

// GitHub's own star button (ghbtns.com), shown with the live star count.
const props = withDefaults(defineProps<{ size?: 'small' | 'large' }>(), { size: 'small' })

const src = computed(
  () =>
    'https://ghbtns.com/github-btn.html?user=formosa-ventures&repo=arneb&type=star&count=true' +
    (props.size === 'large' ? '&size=large' : '')
)
const width = computed(() => (props.size === 'large' ? 140 : 90))
const height = computed(() => (props.size === 'large' ? 30 : 20))
</script>

<template>
  <span class="github-star">
    <iframe
      :src="src"
      :width="width"
      :height="height"
      frameborder="0"
      scrolling="0"
      title="Star Arneb on GitHub"
      loading="lazy"
    />
  </span>
</template>
