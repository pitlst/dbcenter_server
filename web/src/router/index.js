import { createRouter, createWebHistory } from 'vue-router'

const router = createRouter({
    history: createWebHistory(import.meta.env.BASE_URL),
    routes: [
        {
            path: '/',
            name: '主页',
            component: () => import('../views/Home.vue'),
        },
        {
            path: '/travel',
            name: '差旅看板',
            component: () => import('../views/Travel.vue'),
        },
        {
            path: '/DesignChange',
            name: '设计变更统计',
            component: () => import('../views/DesignChange.vue'),
        },
    ],
})

export default router
