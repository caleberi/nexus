import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
var apiTarget = process.env.VITE_API_PROXY_TARGET || 'http://localhost:3000';
export default defineConfig({
    plugins: [react()],
    server: {
        proxy: {
            '/api': {
                target: apiTarget,
                changeOrigin: true,
            },
        },
    },
    build: {
        outDir: 'dist',
        emptyOutDir: true,
    },
});
