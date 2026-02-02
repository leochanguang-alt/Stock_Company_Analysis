#!/usr/bin/env node
const http = require('http');
const fs = require('fs');
const { spawn } = require('child_process');
const path = require('path');

const ROOT_DIR = path.resolve(__dirname, '..');
const PORT = 8000;

// MIME types for static files
const MIME_TYPES = {
    '.html': 'text/html',
    '.css': 'text/css',
    '.js': 'application/javascript',
    '.json': 'application/json',
    '.csv': 'text/csv',
    '.png': 'image/png',
    '.jpg': 'image/jpeg',
    '.svg': 'image/svg+xml',
};

function serveStaticFile(res, filePath) {
    const fullPath = path.join(ROOT_DIR, filePath);
    const ext = path.extname(fullPath);
    const contentType = MIME_TYPES[ext] || 'text/plain';
    
    fs.readFile(fullPath, (err, data) => {
        if (err) {
            res.writeHead(404, { 'Content-Type': 'text/plain' });
            res.end('File not found');
            return;
        }
        res.writeHead(200, { 
            'Content-Type': contentType,
            'Access-Control-Allow-Origin': '*'
        });
        res.end(data);
    });
}

function runCommand(cmd, args) {
    return new Promise((resolve, reject) => {
        const proc = spawn(cmd, args, {
            cwd: ROOT_DIR,
            stdio: ['ignore', 'pipe', 'pipe'],
        });
        let stdout = '';
        let stderr = '';
        proc.stdout.on('data', (d) => { stdout += d.toString(); });
        proc.stderr.on('data', (d) => { stderr += d.toString(); });
        proc.on('close', (code) => {
            if (code === 0) return resolve({ stdout, stderr });
            reject(new Error(stderr || stdout || `Exit code ${code}`));
        });
    });
}

function sendJson(res, code, payload) {
    const body = JSON.stringify(payload);
    res.writeHead(code, {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'POST, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type',
    });
    res.end(body);
}

const server = http.createServer(async (req, res) => {
    // Handle CORS preflight
    if (req.method === 'OPTIONS') {
        res.writeHead(204, {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'POST, GET, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type',
        });
        return res.end();
    }

    // Handle GET requests for static files
    if (req.method === 'GET') {
        let filePath = req.url === '/' ? '/koyfin_dashboard_002508.html' : req.url;
        // Remove query string if present
        filePath = filePath.split('?')[0];
        return serveStaticFile(res, filePath);
    }

    if (req.method !== 'POST' || req.url !== '/akshare/fetch') {
        return sendJson(res, 404, { error: 'Not Found' });
    }

    let body = '';
    req.on('data', (chunk) => { body += chunk.toString(); });
    req.on('end', async () => {
        let payload = {};
        try {
            payload = JSON.parse(body || '{}');
        } catch {
            payload = {};
        }
        const symbol = String(payload.symbol || '').trim();
        if (!symbol) {
            return sendJson(res, 400, { error: 'Missing symbol' });
        }

        try {
            const python = process.env.PYTHON_BIN || 'python3';
            console.log(`[${new Date().toISOString()}] 开始处理 ${symbol}...`);
            console.log(`[${new Date().toISOString()}] 执行 fetch_stock_data.py...`);
            await runCommand(python, [path.join('scripts', 'fetch_stock_data.py'), `--symbol=${symbol}`]);
            console.log(`[${new Date().toISOString()}] 执行 upload_stock_data.py...`);
            await runCommand(python, [path.join('scripts', 'upload_stock_data.py'), `--symbol=${symbol}`]);
            console.log(`[${new Date().toISOString()}] 完成 ${symbol}`);
            return sendJson(res, 200, { message: `下载并上传完成: ${symbol}` });
        } catch (err) {
            console.error(`[${new Date().toISOString()}] 错误: ${err.message}`);
            return sendJson(res, 500, { error: err.message });
        }
    });
});

server.listen(PORT, '0.0.0.0', () => {
    console.log(`AKShare fetch server running at http://localhost:${PORT}`);
});
