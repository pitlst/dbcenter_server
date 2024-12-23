<script setup>
import { ref } from 'vue'
const greeting = ref('Hello World!')
</script>

<style>
.greeting {
    color: red;
    font-weight: bold;
}
</style>

<template>
    <p class="greeting">{{ greeting }}</p>
    <SelectBI></SelectBI>
</template>