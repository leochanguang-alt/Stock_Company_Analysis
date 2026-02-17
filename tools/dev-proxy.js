#!/usr/bin/env node
const http = require('http');
const https = require('https');
const { URL } = require('url');

const TARGET = 'https://bsa.buiservice.com';
const PORT = process.env.PORT || 8787;

function proxyRequest(req, res) {
  const targetUrl = new URL(req.url, TARGET);
  const isHttps = targetUrl.protocol === 'https:';
  const client = isHttps ? https : http;

  const options = {
    method: req.method,
    headers: {
      ...req.headers,
      host: targetUrl.host,
      origin: TARGET,
      referer: TARGET + '/',
    },
  };

  const upstream = client.request(targetUrl, options, (upstreamRes) => {
    res.writeHead(upstreamRes.statusCode || 502, {
      ...upstreamRes.headers,
      'access-control-allow-origin': '*',
      'access-control-allow-headers': '*',
      'access-control-allow-methods': 'GET,POST,PUT,PATCH,DELETE,OPTIONS',
    });
    upstreamRes.pipe(res);
  });

  upstream.on('error', (err) => {
    res.writeHead(502, {
      'content-type': 'text/plain; charset=utf-8',
      'access-control-allow-origin': '*',
    });
    res.end(`Proxy error: ${err.message}`);
  });

  req.pipe(upstream);
}

const server = http.createServer((req, res) => {
  if (req.method === 'OPTIONS') {
    res.writeHead(204, {
      'access-control-allow-origin': '*',
      'access-control-allow-headers': '*',
      'access-control-allow-methods': 'GET,POST,PUT,PATCH,DELETE,OPTIONS',
    });
    return res.end();
  }
  if (req.url.startsWith('/api/')) return proxyRequest(req, res);
  res.writeHead(404, { 'content-type': 'text/plain; charset=utf-8' });
  res.end('Only /api/* is proxied.');
});

server.listen(PORT, () => {
  console.log(`Dev proxy listening on http://localhost:${PORT}`);
  console.log(`Forwarding /api/* to ${TARGET}`);
});
