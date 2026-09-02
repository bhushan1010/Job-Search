import { Actor, log } from 'apify';
import { ApifyClient } from 'apify-client';

await Actor.init();

try {
    const rawInput = (await Actor.getInput()) || {};
    
    // Support being triggered via Apify Webhook (where payload contains resource.defaultDatasetId)
    // or direct invocation with datasetId
    const datasetId = rawInput.datasetId 
        || rawInput.resource?.defaultDatasetId 
        || rawInput.eventData?.defaultDatasetId
        || rawInput.defaultDatasetId;

    const apifyToken = rawInput.apifyToken || process.env.APIFY_TOKEN;
    const telegramBotToken = rawInput.telegramBotToken || process.env.TELEGRAM_BOT_TOKEN;
    const telegramChatId = rawInput.telegramChatId || process.env.TELEGRAM_CHAT_ID;
    const slackWebhookUrl = rawInput.slackWebhookUrl || process.env.SLACK_WEBHOOK_URL;

    const sourceLabel = rawInput.sourceLabel || rawInput.resource?.actorId || 'Job Search Pipeline';
    const rawStoreName = rawInput.storeName || 'job-pipeline-seen-store';
    const storeName = rawStoreName.toLowerCase().replace(/[^a-z0-9-]/g, '-').replace(/^-+|-+$/g, '') || 'job-pipeline-seen-store';
    const maxSeenHistory = rawInput.maxSeenHistory || 10000;

    const excludeRegexStr = rawInput.excludeTitleRegex 
        || '\\b(sr\\.?|senior|lead|principal|staff|director|vp|vice president|head of|manager|chief|architect|team lead|5\\+|6\\+|7\\+|8\\+|10\\+|5 to 10|5-10|4\\+|4-8)\\b';
    const excludeRegex = new RegExp(excludeRegexStr, 'i');

    const entryLevelPositiveRegex = /\b(fresher|intern|internship|graduate|trainee|entry[- ]level|junior|jr\.?|associate|0[- ]1\s*years?|0[- ]2\s*years?)\b/i;

    log.info('Pipeline runner started', {
        datasetId,
        sourceLabel,
        storeName,
        hasApifyToken: Boolean(apifyToken),
        hasTelegram: Boolean(telegramBotToken && telegramChatId),
        hasSlack: Boolean(slackWebhookUrl),
    });

    if (!datasetId) {
        log.warning('No datasetId provided in input or webhook payload. If running standalone test, pass "datasetId": "<YOUR_DATASET_ID>".');
        await Actor.exit({ exitCode: 0 });
    }

    // 1. Fetch raw items from the scraper dataset
    const client = new ApifyClient({
        token: apifyToken,
    });

    const datasetClient = client.dataset(datasetId);
    const { items: rawItems } = await datasetClient.listItems({ limit: 1000 });

    log.info(`Fetched ${rawItems.length} raw job items from dataset ${datasetId}`);

    if (rawItems.length === 0) {
        log.info('Dataset is empty. Exiting without updates.');
        await Actor.exit({ exitCode: 0 });
    }

    // 2. Open persistent Key-Value Store for deduplication
    const store = await Actor.openKeyValueStore(storeName);
    const storeRecordKey = 'SEEN_JOB_IDS_MAP';
    const seenMap = (await store.getValue(storeRecordKey)) || {};

    log.info(`Loaded ${Object.keys(seenMap).length} historical seen job IDs from store '${storeName}'`);

    // 3. Process, normalize, filter, and deduplicate
    const newFilteredJobs = [];
    const skippedSenior = [];
    const skippedDuplicate = [];

    // Helper: Clean tracking parameters from LinkedIn / Indeed URLs
    const cleanUrl = (rawUrl) => {
        if (!rawUrl) return '';
        try {
            const parsed = new URL(rawUrl);
            return `${parsed.origin}${parsed.pathname}`;
        } catch {
            return rawUrl.split('?')[0];
        }
    };

    for (const item of rawItems) {
        const title = (item.title || item.jobTitle || item.position || '').trim();
        const company = (item.companyName || item.company || item.company_name || 'Confidential').trim();
        const location = (item.location || item.jobLocation || item.formattedLocation || item.place || 'Pune / India').trim();
        const rawUrl = item.jobUrl || item.url || item.link || (item.id ? `https://www.linkedin.com/jobs/view/${item.id}` : '');
        const url = cleanUrl(rawUrl);
        const postedAt = (item.postedTime || item.postedAt || item.date || item.postedDate || 'Recently').trim();
        const salary = (item.salary || item.extractedSalary || item.compensation || '').trim();
        const seniority = (item.seniorityLevel || item.experienceLevel || '').trim();
        const description = (item.description || item.jobDescription || item.snippet || '').trim();

        const uniqueId = url || item.id || item.jobKey || `${title}::${company}`.toLowerCase().replace(/\s+/g, '-');

        // Check if senior/lead role (Exclude if matches senior regex, unless explicitly an Associate/Fresher/Junior title)
        const isSenior = excludeRegex.test(title);
        const isExplicitFresher = entryLevelPositiveRegex.test(title);

        if (isSenior && !isExplicitFresher) {
            skippedSenior.push({ title, company });
            continue;
        }

        // Deduplication check
        if (seenMap[uniqueId]) {
            skippedDuplicate.push(uniqueId);
            continue;
        }

        // Mark as seen
        seenMap[uniqueId] = Date.now();

        const isFresherFriendly = isExplicitFresher || entryLevelPositiveRegex.test(description) || /entry/i.test(seniority);

        newFilteredJobs.push({
            id: uniqueId,
            title,
            company,
            location,
            url: url || rawUrl,
            postedAt,
            salary: salary || undefined,
            seniorityLevel: seniority || undefined,
            isFresherFriendly,
            sourceKeyword: sourceLabel,
            discoveredAt: new Date().toISOString(),
        });
    }

    log.info(`Deduplication & Filter Summary:`, {
        totalScraped: rawItems.length,
        skippedSenior: skippedSenior.length,
        skippedDuplicates: skippedDuplicate.length,
        newJobsFound: newFilteredJobs.length,
    });

    // 4. Prune seenMap if it exceeds max size (remove oldest entries)
    const seenEntries = Object.entries(seenMap);
    if (seenEntries.length > maxSeenHistory) {
        seenEntries.sort((a, b) => a[1] - b[1]);
        const trimmedMap = Object.fromEntries(seenEntries.slice(seenEntries.length - maxSeenHistory));
        await store.setValue(storeRecordKey, trimmedMap);
        log.info(`Pruned seen ID store down to ${maxSeenHistory} records.`);
    } else {
        await store.setValue(storeRecordKey, seenMap);
    }

    // 5. Push new jobs to Actor dataset
    if (newFilteredJobs.length > 0) {
        await Actor.pushData(newFilteredJobs);
        log.info(`Pushed ${newFilteredJobs.length} new items to default dataset.`);
    }

    // 6. Append to Google Sheets (via SheetDB or Webhook)
    if (googleSheetWebhookUrl && newFilteredJobs.length > 0) {
        log.info(`Appending ${newFilteredJobs.length} new jobs to Google Sheets via SheetDB...`);
        try {
            const rows = newFilteredJobs.map(job => ({
                Date: new Date().toLocaleDateString('en-IN'),
                Time: new Date().toLocaleTimeString('en-IN'),
                Title: job.title,
                Company: job.company,
                Experience: job.seniorityLevel || (job.isFresherFriendly ? 'Fresher / Entry level' : 'Entry level'),
                Location: job.location,
                Salary: job.salary || 'Not disclosed',
                Url: job.url,
                Source: job.sourceKeyword || sourceLabel,
                Status: 'New'
            }));

            // SheetDB expects { data: [ ...rows ] }
            const payload = googleSheetWebhookUrl.includes('sheetdb.io') 
                ? { data: rows } 
                : { jobs: rows, data: rows };

            const sheetRes = await fetch(googleSheetWebhookUrl, {
                method: 'POST',
                headers: { 
                    'Accept': 'application/json',
                    'Content-Type': 'application/json' 
                },
                body: JSON.stringify(payload),
            });

            if (sheetRes.ok) {
                const resJson = await sheetRes.json().catch(() => ({}));
                log.info('Successfully appended rows to Google Sheet:', resJson);
            } else {
                const sheetErr = await sheetRes.text();
                log.error(`Google Sheet append failed: ${sheetRes.status} - ${sheetErr}`);
            }
        } catch (sheetErr) {
            log.error(`Error sending to Google Sheet: ${sheetErr.message}`);
        }
    }

    // 7. Deliver to Telegram (Chunked to send ALL jobs without hitting character limit)
    if (telegramBotToken && telegramChatId && newFilteredJobs.length > 0) {
        log.info(`Delivering all ${newFilteredJobs.length} job alerts to Telegram in chunked messages...`);

        // Helper for relative time formatting
        const getRelativeTime = (isoOrText) => {
            if (!isoOrText) return 'Recently';
            const timestamp = Date.parse(isoOrText);
            if (isNaN(timestamp)) return isoOrText;
            const diffMs = Date.now() - timestamp;
            const diffMins = Math.floor(diffMs / (1000 * 60));
            if (diffMins < 60) return `${diffMins}m ago`;
            const diffHours = Math.floor(diffMins / 60);
            if (diffHours < 24) return `${diffHours}h ago`;
            const diffDays = Math.floor(diffHours / 24);
            return `${diffDays}d ago`;
        };

        const CHUNK_SIZE = 8; // 8 jobs per message to stay comfortably within Telegram's 4096 char limit
        const totalChunks = Math.ceil(newFilteredJobs.length / CHUNK_SIZE);

        for (let chunkIdx = 0; chunkIdx < totalChunks; chunkIdx++) {
            const chunkJobs = newFilteredJobs.slice(chunkIdx * CHUNK_SIZE, (chunkIdx + 1) * CHUNK_SIZE);
            
            let message = `🎯 <b>${newFilteredJobs.length} New Jobs: ${sourceLabel}</b> (Batch ${chunkIdx + 1}/${totalChunks})\n`;
            message += `━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n`;

            for (const job of chunkJobs) {
                const fresherBadge = job.isFresherFriendly ? ' 🟢 [Fresher Friendly]' : '';
                const expText = job.seniorityLevel || (job.isFresherFriendly ? 'Fresher / Entry level (0-2 yrs)' : 'Entry level');
                const salaryText = job.salary ? job.salary : 'Not disclosed';
                const timeAgo = getRelativeTime(job.postedAt);

                message += `📌 <b>Title:</b> <a href="${job.url}">${job.title}</a>${fresherBadge}\n`;
                message += `🏢 <b>Company:</b> ${job.company}\n`;
                message += `🎓 <b>Experience:</b> ${expText}\n`;
                message += `📍 <b>Location:</b> ${job.location}\n`;
                message += `💰 <b>Salary:</b> ${salaryText}\n`;
                message += `🔗 <b>Link:</b> ${job.url}\n`;
                message += `🕒 <b>Posted:</b> ${timeAgo}\n`;
                message += `──────────────────────────\n\n`;
            }

            const telegramApiUrl = `https://api.telegram.org/bot${telegramBotToken}/sendMessage`;

            await fetch(telegramApiUrl, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    chat_id: telegramChatId,
                    text: message,
                    parse_mode: 'HTML',
                    disable_web_page_preview: true,
                }),
            });

            // Small 300ms pause between batches to respect Telegram rate limits
            await new Promise(res => setTimeout(res, 300));
        }

        log.info(`Successfully posted all ${newFilteredJobs.length} jobs across ${totalChunks} messages to Telegram.`);
    } else if (telegramBotToken && telegramChatId && newFilteredJobs.length === 0) {
        log.info('0 new jobs after deduplication. Telegram alert suppressed.');
    }

    // 7. Deliver to Slack (if configured)
    if (slackWebhookUrl && newFilteredJobs.length > 0) {
        log.info(`Delivering ${newFilteredJobs.length} new job alerts to Slack...`);

        const displayJobs = newFilteredJobs.slice(0, 10);
        const remainingCount = newFilteredJobs.length - displayJobs.length;

        const blocks = [
            {
                type: 'header',
                text: {
                    type: 'plain_text',
                    text: `🎯 ${newFilteredJobs.length} New Job${newFilteredJobs.length > 1 ? 's' : ''} Found: ${sourceLabel}`,
                    emoji: true,
                },
            },
            {
                type: 'context',
                elements: [
                    {
                        type: 'mrkdwn',
                        text: `*Target:* Entry-Level / Fresher (Pune & Remote/India)`,
                    },
                ],
            },
            {
                type: 'divider',
            },
        ];

        for (const job of displayJobs) {
            const fresherBadge = job.isFresherFriendly ? ' 🟢 `Fresher/Entry-Level`' : '';
            const salaryText = job.salary ? ` • 💰 _${job.salary}_` : '';
            blocks.push({
                type: 'section',
                text: {
                    type: 'mrkdwn',
                    text: `*<${job.url}|${job.title}>*${fresherBadge}\n🏢 *${job.company}* • 📍 *${job.location}*${salaryText}\n🕒 _Posted: ${job.postedAt}_`,
                },
            });
        }

        if (remainingCount > 0) {
            blocks.push({
                type: 'context',
                elements: [
                    {
                        type: 'mrkdwn',
                        text: `➕ _...and *${remainingCount} more* new jobs found in this run._`,
                    },
                ],
            });
        }

        await fetch(slackWebhookUrl, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                text: `🎯 ${newFilteredJobs.length} New Jobs found for "${sourceLabel}"`,
                blocks,
            }),
        });
    }

} catch (err) {
    log.error(`Actor failed with error: ${err.message}`, { stack: err.stack });
    throw err;
} finally {
    await Actor.exit();
}
