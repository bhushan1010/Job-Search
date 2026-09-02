// test-dedup.js - Offline test script to verify filtering and deduplication logic
import { readFileSync } from 'fs';

const excludeRegex = new RegExp(
    '\\b(sr\\.?|senior|lead|principal|staff|director|vp|vice president|head of|manager|chief|architect|team lead|5\\+|6\\+|7\\+|8\\+|10\\+|5 to 10|5-10|4\\+|4-8)\\b',
    'i'
);

const entryLevelPositiveRegex = /\b(fresher|intern|internship|graduate|trainee|entry[- ]level|junior|jr\.?|associate|0[- ]1\s*years?|0[- ]2\s*years?)\b/i;

const mockScrapedJobs = [
    {
        title: "Senior AI/ML Engineer",
        company: "Global Tech Inc",
        location: "Pune, Maharashtra",
        url: "https://www.linkedin.com/jobs/view/1001?trackingId=abc123xyz",
        postedTime: "2 hours ago",
        seniorityLevel: "Mid-Senior level"
    },
    {
        title: "Junior Machine Learning Engineer",
        company: "Innovate AI",
        location: "Pune (Hybrid)",
        url: "https://www.linkedin.com/jobs/view/1002?trackingId=def456",
        postedTime: "4 hours ago",
        seniorityLevel: "Entry level"
    },
    {
        title: "Associate Software Engineer - GenAI",
        company: "NextGen Systems",
        location: "Pune, India",
        url: "https://www.linkedin.com/jobs/view/1003",
        postedTime: "5 hours ago",
        seniorityLevel: "Entry level"
    },
    {
        title: "Staff DevOps Engineer",
        company: "CloudCore",
        location: "Remote - India",
        url: "https://www.linkedin.com/jobs/view/1004",
        postedTime: "1 hour ago",
        seniorityLevel: "Director"
    },
    {
        title: "Backend Engineer (0-1 YOE / Fresher)",
        company: "Pune Fintech Labs",
        location: "Pune",
        url: "https://www.linkedin.com/jobs/view/1005",
        postedTime: "30 mins ago",
        seniorityLevel: "Associate"
    }
];

console.log("=== Testing Fresher Filter & Dedup Rules ===");

const cleanUrl = (rawUrl) => {
    try {
        const parsed = new URL(rawUrl);
        return `${parsed.origin}${parsed.pathname}`;
    } catch {
        return rawUrl.split('?')[0];
    }
};

const seenMap = {};

function processJobs(jobs) {
    const passed = [];
    const skippedSenior = [];
    const skippedDuplicate = [];

    for (const job of jobs) {
        const title = job.title.trim();
        const url = cleanUrl(job.url);
        const uniqueId = url || `${title}::${job.company}`;

        const isSenior = excludeRegex.test(title);
        const isExplicitFresher = entryLevelPositiveRegex.test(title);

        if (isSenior && !isExplicitFresher) {
            skippedSenior.push(job.title);
            continue;
        }

        if (seenMap[uniqueId]) {
            skippedDuplicate.push(job.title);
            continue;
        }

        seenMap[uniqueId] = Date.now();
        passed.push({
            title: job.title,
            company: job.company,
            location: job.location,
            url,
            isFresherFriendly: isExplicitFresher || entryLevelPositiveRegex.test(job.seniorityLevel)
        });
    }

    return { passed, skippedSenior, skippedDuplicate };
}

// Run 1: First batch
console.log("\n--- Run 1: Initial Discovery ---");
const run1 = processJobs(mockScrapedJobs);
console.log(`✅ Qualified Entry-Level Jobs (${run1.passed.length}):`);
run1.passed.forEach(j => console.log(`  - [${j.isFresherFriendly ? '🟢 FRESHER' : 'ROLE'}] ${j.title} @ ${j.company} (${j.location})`));
console.log(`🚫 Filtered Out Senior/Lead (${run1.skippedSenior.length}):`);
run1.skippedSenior.forEach(t => console.log(`  - ${t}`));

// Run 2: Second batch with overlapping and 1 new job
console.log("\n--- Run 2: Re-run with Duplicates + 1 New Listing ---");
const newJob = {
    title: "Full Stack Developer (Fresher)",
    company: "RapidScale",
    location: "Pune, India",
    url: "https://www.linkedin.com/jobs/view/1006?refId=111",
    postedTime: "10 mins ago",
    seniorityLevel: "Entry level"
};
const run2 = processJobs([...mockScrapedJobs, newJob]);
console.log(`✅ New Jobs Passed (${run2.passed.length}):`);
run2.passed.forEach(j => console.log(`  - ${j.title} @ ${j.company}`));
console.log(`🔁 Duplicates Blocked (${run2.skippedDuplicate.length}):`);
run2.skippedDuplicate.forEach(t => console.log(`  - ${t}`));

console.log("\n=== Test Completed Successfully ===");
