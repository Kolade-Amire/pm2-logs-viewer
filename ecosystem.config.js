module.exports = {
  apps: [
    {
      name: 'pm2-log-viewer',
      script: 'dist/server.js',
      env: {
        PORT: 4000,
        VIEWER_TOKEN: 'replace-me'
      }
    }
  ]
};
