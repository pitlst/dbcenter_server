<template>
    <div ref="chart" style="width: 100%; height: 400px;"></div>
</template>

<script>
import * as echarts from 'echarts';

export default {
    name: 'SelectBI',
    data() {
        return {
            chart: null,
        };
    },
    mounted() {
        this.initChart();
    },
    methods: {
        initChart() {
            this.chart = echarts.init(this.$refs.chart);
            const option = {
                title: {
                    text: '折线图示例',
                },
                tooltip: {
                    trigger: 'axis',
                },
                xAxis: {
                    type: 'category',
                    data: ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'],
                },
                yAxis: {
                    type: 'value',
                },
                series: [
                    {
                        name: '销量',
                        type: 'line',
                        data: [120, 200, 150, 80, 70, 110, 130],
                    },
                ],
            };
            this.chart.setOption(option);
        },
    },
    beforeDestroy() {
        if (this.chart) {
            this.chart.dispose();
        }
    },
};
</script>

<style scoped>
/* 你可以在这里添加样式 */
</style>