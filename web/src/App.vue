<script setup>
import { RouterLink, RouterView } from 'vue-router'
import { ref } from 'vue'

const isDarkMode = ref(false)

const toggleDarkMode = () => {
    isDarkMode.value = !isDarkMode.value
    if (isDarkMode.value) {
        document.documentElement.classList.add('dark')
    } else {
        document.documentElement.classList.remove('dark')
    }
}
</script>

<template>
    <main>
        <RouterView />
        <el-button @click="toggleDarkMode" class="dark-mode-toggle">
            {{ isDarkMode ? 'Light Mode' : 'Dark Mode' }}
        </el-button>
    </main>
</template>

<style scoped>
.dark-mode-toggle {
    position: fixed;
    top: 10px;
    right: 10px;
    padding: 10px;
    background-color: #333;
    color: #fff;
    border: none;
    border-radius: 5px;
    cursor: pointer;
}

.dark-mode-toggle:hover {
    background-color: #555;
}
</style>

<!-- <style>
.dark {
    background-color: #121212;
    color: #ffffff;
}
</style> -->