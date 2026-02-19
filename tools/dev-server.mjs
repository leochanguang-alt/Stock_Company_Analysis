#!/usr/bin/env node
/**
 * Local dev server:
 *  - Serves static files from public/
 *  - Runs local API handlers from api/ (ESM export default)
 *  - Falls back to proxying unknown /api/* to production
 */
import http from 'node:http';
import https from 'node:https';
import fs from 'node:fs';
import path from 'node:path';
import { URL, fileURLToPath } from 'node:url';
import dotenv from 'dotenv';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');

dotenv.config({ path: path.join(ROOT, '.env') });

const PORT = Number(process.env.DEV_PORT) || 5173;
const PROD = 'https://bsa.buiservice.com';
const PUBLIC = path.join(ROOT, 'public');
const API_DIR = path.join(ROOT, 'api');

// Only these APIs run locally; everything else proxies to production
const LOCAL_APIS = new Set([
  'fetch-hk-data',
  'hk-analysis',
]);

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.css':  'text/css; charset=utf-8',
  '.js':   'text/javascript; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.png':  'image/png',
  '.jpg':  'image/jpeg',
  '.svg':  'image/svg+xml',
  '.ico':  'image/x-icon',
  '.woff': 'font/woff',
  '.woff2':'font/woff2',
};

function serveStatic(filePath, res) {
  if (!fs.existsSync(filePath)) return false;
  const stat = fs.statSync(filePath);
  if (stat.isDirectory()) {
    const idx = path.join(filePath, 'index.html');
    if (fs.existsSync(idx)) { filePath = idx; } else return false;
  }
  const ext = path.extname(filePath).toLowerCase();
  res.writeHead(200, {
    'Content-Type': MIME[ext] || 'application/octet-stream',
    'Content-Length': fs.statSync(filePath).size,
    'Cache-Control': 'no-cache',
  });
  fs.createReadStream(filePath).pipe(res);
  return true;
}

function parseQuery(url) {
  const q = {};
  const idx = url.indexOf('?');
  if (idx < 0) return q;
  new URLSearchParams(url.slice(idx + 1)).forEach((v, k) => { q[k] = v; });
  return q;
}

async function handleLocalApi(apiName, req, res) {
  const filePath = path.join(API_DIR, apiName + '.js');
  if (!fs.existsSync(filePath)) return false;

  try {
    const mod = await import(filePath);
    const handler = mod.default;
    if (typeof handler !== 'function') return false;

    const fakeReq = {
      method: req.method,
      url: req.url,
      headers: req.headers,
      query: parseQuery(req.url),
    };

    const fakeRes = {
      _statusCode: 200,
      _headers: {},
      _body: null,
      _redirect: null,
      setHeader(k, v) { this._headers[k.toLowerCase()] = v; return this; },
      writeHead(code, hdrs) { this._statusCode = code; Object.assign(this._headers, hdrs); return this; },
      status(code) { this._statusCode = code; return this; },
      json(data) { this._body = JSON.stringify(data); return this; },
      end(body) { if (body !== undefined) this._body = body; return this; },
      redirect(code, url) { 
        this._statusCode = code; 
        this._redirect = url; 
        this._headers['location'] = url; 
        return this; 
      },
    };

    await handler(fakeReq, fakeRes);

    // Handle redirect
    if (fakeRes._redirect) {
      const redirectUrl = fakeRes._redirect;
      if (redirectUrl.startsWith('/api/')) {
        // Internal redirect to another local API
        const redirectApiName = redirectUrl.replace(/^\/api\//, '').split('?')[0];
        const redirectQuery = parseQuery(redirectUrl);
        const redirectReq = { ...fakeReq, url: redirectUrl, query: redirectQuery };
        const redirectRes = {
          _statusCode: 200,
          _headers: {},
          _body: null,
          _redirect: null,
          setHeader(k, v) { this._headers[k.toLowerCase()] = v; return this; },
          writeHead(code, hdrs) { this._statusCode = code; Object.assign(this._headers, hdrs); return this; },
          status(code) { this._statusCode = code; return this; },
          json(data) { this._body = JSON.stringify(data); return this; },
          end(body) { if (body !== undefined) this._body = body; return this; },
          redirect(code, url) { this._statusCode = code; this._redirect = url; this._headers['location'] = url; return this; },
        };
        const redirectFilePath = path.join(API_DIR, redirectApiName + '.js');
        if (fs.existsSync(redirectFilePath)) {
          const redirectMod = await import(redirectFilePath);
          const redirectHandler = redirectMod.default;
          if (typeof redirectHandler === 'function') {
            await redirectHandler(redirectReq, redirectRes);
            const outHeaders = {
              'access-control-allow-origin': '*',
              'content-type': 'application/json; charset=utf-8',
              ...redirectRes._headers,
            };
            res.writeHead(redirectRes._statusCode, outHeaders);
            res.end(redirectRes._body || '');
            return true;
          }
        }
      }
      // External redirect (not handled in dev server)
      res.writeHead(fakeRes._statusCode, { 'Location': redirectUrl });
      res.end();
      return true;
    }

    const outHeaders = {
      'access-control-allow-origin': '*',
      'content-type': 'application/json; charset=utf-8',
      ...fakeRes._headers,
    };
    res.writeHead(fakeRes._statusCode, outHeaders);
    res.end(fakeRes._body || '');
    return true;
  } catch (err) {
    console.error(`[LOCAL API ERROR] ${apiName}:`, err);
    res.writeHead(500, {
      'content-type': 'application/json; charset=utf-8',
      'access-control-allow-origin': '*',
    });
    res.end(JSON.stringify({ error: err.message }));
    return true;
  }
}

function proxyToProduction(req, res) {
  const targetUrl = new URL(req.url, PROD);
  const options = {
    method: req.method,
    headers: { ...req.headers, host: targetUrl.host, origin: PROD, referer: PROD + '/' },
  };
  const upstream = https.request(targetUrl, options, (upRes) => {
    res.writeHead(upRes.statusCode || 502, {
      ...upRes.headers,
      'access-control-allow-origin': '*',
    });
    upRes.pipe(res);
  });
  upstream.on('error', (err) => {
    res.writeHead(502, { 'content-type': 'text/plain; charset=utf-8', 'access-control-allow-origin': '*' });
    res.end(`Proxy error: ${err.message}`);
  });
  req.pipe(upstream);
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

  const pathname = new URL(req.url, 'http://localhost').pathname;

  if (pathname.startsWith('/api/')) {
    const apiName = pathname.replace('/api/', '').split('/')[0];
    if (LOCAL_APIS.has(apiName)) {
      console.log(`[LOCAL] ${req.method} ${pathname} → ${apiName}`);
      const handled = await handleLocalApi(apiName, req, res);
      if (handled) return;
    }
    console.log(`[PROXY] ${pathname} → ${PROD}`);
    return proxyToProduction(req, res);
  }

  const filePath = path.join(PUBLIC, pathname === '/' ? 'index.html' : pathname);
  if (serveStatic(filePath, res)) {
    console.log(`[STATIC] ${pathname}`);
    return;
  }

  res.writeHead(404, { 'content-type': 'text/plain; charset=utf-8' });
  res.end('404 Not Found');
});

server.listen(PORT, () => {
  console.log('');
  console.log(`  🚀 Dev server running at http://localhost:${PORT}`);
  console.log(`  📁 Static files: ${PUBLIC}`);
  console.log(`  🔌 Local APIs:   ${API_DIR}`);
  console.log(`  ↗️  Proxy target: ${PROD}`);
  console.log('');
  console.log(`  Open: http://localhost:${PORT}/stock-search.html`);
  console.log('');
});
