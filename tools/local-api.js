#!/usr/bin/env node
const http = require('http');
const { URL, pathToFileURL } = require('url');
const path = require('path');
const dotenv = require('dotenv');

dotenv.config({ path: path.join(__dirname, '..', '.env') });

const PORT = process.env.LOCAL_API_PORT || 8787;

const handlerCache = new Map();

async function loadHandler(name) {
  if (handlerCache.has(name)) return handlerCache.get(name);
  const fileUrl = pathToFileURL(path.join(__dirname, '..', 'api', `${name}.js`)).href;
  const mod = await import(fileUrl);
  const handler = mod?.default || mod?.handler;
  if (typeof handler !== 'function') {
    throw new Error(`Handler ${name} not found`);
  }
  handlerCache.set(name, handler);
  return handler;
}

function createRes(res) {
  let statusCode = 200;
  return {
    status(code) {
      statusCode = code;
      return this;
    },
    json(obj) {
      const body = JSON.stringify(obj);
      res.writeHead(statusCode, {
        'content-type': 'application/json; charset=utf-8',
        'access-control-allow-origin': '*',
        'access-control-allow-headers': '*',
        'access-control-allow-methods': 'GET,POST,PUT,PATCH,DELETE,OPTIONS',
      });
      res.end(body);
    },
  };
}

function parseQuery(reqUrl) {
  const url = new URL(reqUrl, 'http://localhost');
  const query = {};
  for (const [k, v] of url.searchParams.entries()) {
    if (query[k] === undefined) query[k] = v;
    else if (Array.isArray(query[k])) query[k].push(v);
    else query[k] = [query[k], v];
  }
  return { pathname: url.pathname, query };
}

const server = http.createServer(async (req, res) => {
  if (req.method === 'OPTIONS') {
    res.writeHead(204, {
      'access-control-allow-origin': '*',
      'access-control-allow-headers': '*',
      'access-control-allow-methods': 'GET,POST,PUT,PATCH,DELETE,OPTIONS',
    });
    return res.end();
  }

  const { pathname, query } = parseQuery(req.url);
  if (!pathname.startsWith('/api/')) {
    res.writeHead(404, { 'content-type': 'text/plain; charset=utf-8' });
    return res.end('Only /api/* is supported.');
  }

  const route = pathname.replace('/api/', '');
  const supported = new Set(['cn-analysis', 'search-stocks', 'check-stock', 'table-stats']);
  if (!supported.has(route)) {
    res.writeHead(404, { 'content-type': 'text/plain; charset=utf-8' });
    return res.end('Unsupported API route.');
  }

  try {
    const handler = await loadHandler(route);
    const reqShim = { query };
    const resShim = createRes(res);
    await handler(reqShim, resShim);
  } catch (err) {
    res.writeHead(500, { 'content-type': 'application/json; charset=utf-8' });
    res.end(JSON.stringify({ error: String(err?.message || err) }));
  }
});

server.listen(PORT, () => {
  console.log(`Local API listening on http://localhost:${PORT}`);
  console.log('Routes: /api/cn-analysis, /api/search-stocks, /api/check-stock, /api/table-stats');
});
