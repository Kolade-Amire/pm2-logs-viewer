const fs = require('fs');
const path = require('path');

// The real token lives in a gitignored .env next to this file (copy
// .env.example and fill it in on the server) — no secrets in version control.
// Shell env wins over .env so one-off overrides still work.
function loadDotEnv() {
  const file = path.join(__dirname, '.env');
  const env = {};
  if (!fs.existsSync(file)) return env;
  for (const line of fs.readFileSync(file, 'utf8').split('\n')) {
    const m = line.match(/^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*?)\s*$/);
    if (m && !line.trimStart().startsWith('#')) env[m[1]] = m[2];
  }
  return env;
}

const dotEnv = loadDotEnv();

module.exports = {
  apps: [
    {
      name: 'pm2-log-viewer',
      script: 'dist/server.js',
      env: {
        PORT: process.env.PORT || dotEnv.PORT || 4000,
        VIEWER_TOKEN: process.env.VIEWER_TOKEN || dotEnv.VIEWER_TOKEN || ''
      }
    }
  ]
};
