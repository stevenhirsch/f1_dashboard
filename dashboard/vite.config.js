import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  // Base path must match the GitHub repository name for GitHub Pages
  base: '/f1_dashboard/',
  server: {
    proxy: {
      // Forwards /anthropic/* → https://api.anthropic.com/* server-to-server (bypasses CORS)
      // Dev only — production uses VITE_ANTHROPIC_PROXY_URL pointing to a Cloudflare Worker
      '/anthropic': {
        target: 'https://api.anthropic.com',
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/anthropic/, ''),
        configure: (proxy) => {
          proxy.on('proxyReq', (proxyReq, req) => {
            // Re-forward auth headers (http-proxy may drop custom headers)
            const apiKey = req.headers['x-api-key']
            if (apiKey) proxyReq.setHeader('x-api-key', Array.isArray(apiKey) ? apiKey[0] : apiKey)
            const version = req.headers['anthropic-version']
            if (version) proxyReq.setHeader('anthropic-version', Array.isArray(version) ? version[0] : version)
            // Strip Origin/Referer so Anthropic treats this as server-to-server, not a browser request
            proxyReq.removeHeader('origin')
            proxyReq.removeHeader('referer')
          })
        },
      },
    },
  },
})
