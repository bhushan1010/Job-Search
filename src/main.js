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

    const slackWebhookUrl = rawInput.slackWebhookUrl || process.env.SLACK_WEBHOOK_URL;
    const sourceLabel = rawInput.sourceLabel || rawInput.resource?.actorId || 'Job Search Pipeline';
    const storeName = rawInput.storeName || 'JOB_PIPELINE_SEEN_STORE';
    const maxSeenHistory = rawInput.maxSeenHistory || 10000;

    const excludeRegexStr = rawInput.excludeTitleRegex 
        || '\\b(sr\\.?|senior|lead|principal|staff|director|vp|vice president|head of|manager|chief|architect|team lead|5\\+|6\\+|7\\+|8\\+|10\\+|5 to 10|5-10|4\\+|4-8)\\b';
    const excludeRegex = new RegExp(excludeRegexStr, 'i');

    const entryLevelPositiveRegex = /\b(fresher|intern|internship|graduate|trainee|entry[- ]level|junior|jr\.?|associate|0[- ]1\s*years?|0[- ]2\s*years?)\b/i;

    log.info('Pipeline runner started', {
        datasetId,
        sourceLabel,
        storeName,
        hasSlackWebhook: Boolean(slackWebhookUrl),
    });

    if (!datasetId) {
        log.warning('No datasetId provided in input or webhook payload. If running standalone test, pass "datasetId": "<YOUR_DATASET_ID>".');
        await Actor.exit({ exitCode: 0 });
    }

    // 1. Fetch raw items from the scraper dataset
    const client = new ApifyClient({
        token: process.env.APIFY_TOKEN,
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
            // Retain path but strip noisy query parameters like trackingId, refId, etc.
            return `${parsed.origin}${parsed.pathname}`;
        } catch {
            return rawUrl.split('?')[0];
        }
    };

    for (const item of rawItems) {
        // Normalize fields across curious_coder/linkedin and borderline/indeed scrapers
        const title = (item.title || item.jobTitle || item.position || '').trim();
        const company = (item.companyName || item.company || item.company_name || 'Confidential').trim();
        const location = (item.location || item.jobLocation || item.formattedLocation || item.place || 'Pune / India').trim();
        const rawUrl = item.jobUrl || item.url || item.link || (item.id ? `https://www.linkedin.com/jobs/view/${item.id}` : '');
        const url = cleanUrl(rawUrl);
        const postedAt = (item.postedTime || item.postedAt || item.date || item.postedDate || 'Recently').trim();
        const salary = (item.salary || item.extractedSalary || item.compensation || '').trim();
        const seniority = (item.seniorityLevel || item.experienceLevel || '').trim();
        const description = (item.description || item.jobDescription || item.snippet || '').trim();

        // Unique ID fingerprint: preference for cleaned URL, then jobId, then combined title+company
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
        seenEntries.sort((a, b) => a[1] - b[1]); // sort ascending by timestamp
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

    // 6. Deliver to Slack (if Webhook URL configured)
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
                        text: `*Target:* Entry-Level / Fresher (Pune & Remote/India) | *Discovered:* <!date^${Math.floor(Date.now() / 1000)}^{date_short_pretty} at {time}|${new Date().toLocaleTimeString()}>`,
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
            const locationText = `📍 *${job.location}*`;
            const companyText = `🏢 *${job.company}*`;

            blocks.push({
                type: 'section',
                text: {
                    type: 'mrkdwn',
                    text: `*<${job.url}|${job.title}>*${fresherBadge}\n${companyText} • ${locationText}${salaryText}\n🕒 _Posted: ${job.postedAt}_`,
                },
            });
        }

        if (remainingCount > 0) {
            blocks.push({
                type: 'context',
                elements: [
                    {
                        type: 'mrkdwn',
                        text: `➕ _...and *${remainingCount} more* new jobs found in this run. Check your Apify dataset for the full list._`,
                    },
                ],
            });
        }

        blocks.push({
            type: 'divider',
        });

        const slackPayload = {
            text: `🎯 ${newFilteredJobs.length} New Jobs found for "${sourceLabel}"`,
            blocks,
        };

        const response = await fetch(slackWebhookUrl, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(slackPayload),
        });

        if (response.ok) {
            log.info('Successfully posted job digest to Slack.');
        } else {
            const errorText = await response.text();
            log.error(`Failed to post to Slack: ${response.status} ${response.statusText} - ${errorText}`);
        }
    } else if (slackWebhookUrl && newFilteredJobs.length === 0) {
        log.info('0 new jobs after deduplication. Slack notification suppressed to prevent channel noise.');
    }

} catch (err) {
    log.error(`Actor failed with error: ${err.message}`, { stack: err.stack });
    throw err;
} finally {
    await Actor.exit();
}
