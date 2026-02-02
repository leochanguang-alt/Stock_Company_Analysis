
require('dotenv').config();
const Parser = require('rss-parser');
const { createClient } = require('@supabase/supabase-js');
const fs = require('fs');
const path = require('path');

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY) {
    console.error('Error: Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY');
    process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);
const parser = new Parser();

// RSSHub local instance URL
const RSSHUB_BASE_URL = 'http://localhost:1200';

async function fetchAndProcessNews(symbol) {
    // 华尔街见闻实时快讯 - 全市场监控
    const wallstreetcnRoute = `/wallstreetcn/live/global`; 
    const wallstreetcnUrl = `${RSSHUB_BASE_URL}${wallstreetcnRoute}`;
    
    console.log(`Fetching WallstreetCN RSS from: ${wallstreetcnUrl}`);

    try {
        const feed = await parser.parseURL(wallstreetcnUrl);
        console.log(`fetched ${feed.items.length} items from ${feed.title}`);

        const newsItems = [];

        for (const item of feed.items) {
            // WallstreetCN RSS items usually have:
            // title: post title
            // content: post content (HTML)
            // pubDate: date string
            // link: url
            
            const pubDate = new Date(item.pubDate);
            
            // Skip future dates (sometimes happens with timezone issues) or very old dates
            if (pubDate > new Date() || pubDate.getFullYear() < 2020) continue;

            const newsItem = {
                symbol: symbol, // Since this is general market news, we tag it with the requested symbol to show up in the dashboard filter, or we could use 'MARKET'
                news_title: item.title || 'No Title',
                news_content: item.contentSnippet || item.content || '',
                published_at: pubDate.toISOString(),
                source: 'WallstreetCN',
                news_url: item.link,
                // New fields for processing later
                grade: null, 
                reason: null,
                mkt_cap_change_1_month: 'n/a'
            };

            newsItems.push(newsItem);
        }

        if (newsItems.length > 0) {
            console.log(`Upserting ${newsItems.length} news items to Supabase...`);
            
            // Use upsert to avoid duplicates based on (symbol, published_at, news_title) if constraint exists
            // Or just insert and let RLS/constraints handle it.
            // But better to check duplicates first or use ON CONFLICT.
            // Our previous schema doesn't seem to have a unique constraint on news URL, but let's try basic upsert on id if we had one.
            // Since we don't have a unique ID from RSS easily, we'll assume the URL is unique.
            
            // Wait, standard supabase insert won't dedup unless we have a unique key. 
            // Let's use the public view which has INSTEAD OF INSERT trigger? 
            // No, the trigger was for market_index_history.
            // cn_company_news table structure:
            // id, symbol, news_title, news_content, published_at, source, news_url, created_at, grade, reason, mkt_cap_change_1_month
            
            // We should check if URL exists to avoid dupes.
            
            for (const news of newsItems) {
                const { data: existing } = await supabase
                    .from('cn_company_news')
                    .select('id')
                    .eq('news_url', news.news_url)
                    .single();

                if (!existing) {
                    const { error } = await supabase.from('cn_company_news').insert(news);
                    if (error) console.error('Insert error:', error.message);
                    else console.log(`Inserted: ${news.news_title}`);
                } else {
                    console.log(`Skipped (exists): ${news.news_title}`);
                }
            }
        }

    } catch (err) {
        console.error('Error fetching/parsing RSS:', err.message);
        if (err.code === 'ECONNREFUSED') {
            console.error('Is RSSHub running on port 1200?');
        }
    }
}

// Main execution
(async () => {
    const symbols = ['002508']; // Add more if needed
    for (const sym of symbols) {
        await fetchAndProcessNews(sym);
    }
})();
