<script setup>
import { RouterLink, RouterView } from 'vue-router'
</script>

<template>
    <header>
        <div class="wrapper">
            <nav>
                <RouterLink to="/DesignChange">设计变更统计</RouterLink>
                <RouterLink to="/travel">差旅</RouterLink>
            </nav>
        </div>
    </header>
    <RouterView />
</template>

<style scoped>
</style>
