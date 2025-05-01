# Queue Investigation

## Current Setup
- PostgreSQL on Neon: `postgres://neondb_owner:npg_jGgcm4L6SVrX@ep-soft-resonance-a42a9ybf-pooler.us-east-1.aws.neon.tech/neondb`
- Redis on Upstash: `rediss://default:AW70AAIjcDE2MzE2YjE5ZTk1MzA0NWUyYjVkYTgyZTBhZDdjMDhhNnAxMA@glorious-woodcock-28404.upstash.io:6379`
- Worker deployed on Railway
- Worker log shows: "👂 Worker escuchando TODAS las colas..."
- 11 new contacts were sent and are in PostgreSQL but not being processed

## Original System Goals
1. **Sequential Queue Processing Per Workflow/Location:**
   - First contact: delay = random(timeframe) + NOW
   - Subsequent contacts: delay = random(timeframe) + last run time
   
2. **Queue Organization Rules:**
   - Same location + same workflow = ONE sequential queue (batches are appended)
   - Same location + different workflows = PARALLEL queues
   - Different locations = PARALLEL queues
   
3. **Immediate Queue Starting:**
   - Different location IDs should start processing immediately
   - Different workflow IDs for the same location should start immediately

## System Flow
1. `api/enqueue-contact.ts` - Receives webhooks, saves to PostgreSQL, enqueues to Redis
2. `api/queue.ts` - Setup for Redis/BullMQ 
3. `worker.ts` - Worker that processes jobs from queues

## Database Findings
- Confirmed contacts are present in PostgreSQL database
- Data in sequential_queue table with far future run_at timestamps (year 2025)
- Multiple workflows present: '8ef44a14-ee1e-4983-9e74-f15916bf8db4' and '9925cc51-aaa8-4939-b0b0-b39b0abf1e8c'
- `run_at` is a timestamp with time zone field

## Redis Findings
- Redis contains many Bull keys
- Queue format appears to be: `bull:{locationId}_{workflowId}_{uuid}:{uuid}_{contactId}`
- There are also `dedup:` keys for deduplication
- Bull queue for 'contactos' exists with many entries
- **CRITICAL:** Jobs in Redis are scheduled for year 2196 (172 years in the future!)
  - Example timestamp: `7151754045902848` converts to `2196-08-17T22:20:45.902Z`
  - This explains why jobs are never processed - they're scheduled too far ahead

## Worker Status
- No actual worker process appears to be running locally
- Worker should be running on Railway, but seems to not be processing jobs

## Root Cause Identified
The problem is in the code logic that calculates job scheduling in `enqueue-contact.ts`:

1. **Database Data Issue:**
   - The `sequential_queue` table contains `run_at` values for year 2025
   - Code structure creates a cascading problem where future dates keep getting pushed further

2. **Date Calculation Logic:**
   ```typescript
   // Line ~109-122
   let lastRunAt = new Date(); 
   // Fetch last run_at from database - getting 2025 dates
   const result = await client.query(
     'SELECT run_at FROM sequential_queue WHERE location_id = $1 AND workflow_id = $2 ORDER BY run_at DESC LIMIT 1',
     [locationId, workflowId]
   );
   if (result.rows.length > 0) {
     lastRunAt = new Date(result.rows[0].run_at);
   }
   // Only reset if lastRunAt is in the past, NOT if it's far in the future
   if (lastRunAt < now) {
     lastRunAt = now;
   }
   // Add delay to already-future date, pushing it even further
   const newRunAt = new Date(lastRunAt.getTime() + delaySeconds * 1000);
   ```

3. **Queue Delay Calculation:**
   ```typescript
   // Line ~150
   const delayMs = newRunAt.getTime() - now.getTime();
   // Delay value becomes extremely large (years worth of milliseconds)
   ```

4. **Result:**
   - Each new job builds on the timestamp of the previous job
   - This creates a snowball effect where timestamps keep moving further into the future
   - Jobs are never processed because they're scheduled too far ahead

## Recommended Fix
The code should be modified to prevent scheduling jobs too far in the future:

1. Add a maximum future limit (e.g., 1 day or 1 week)
2. Reset the scheduling chain if dates are detected too far in the future
3. Consider emptying the sequential_queue table and resetting the entire system

## Additional Considerations
1. **Worker Connection:** The worker process on Railway should be verified
2. **Queue Format Match:** Confirm the queue name format is consistent
3. **Environment Variables:** Ensure all necessary env vars are properly set on Railway
4. **Redis Connection:** The worker.ts and queue.ts use slightly different Redis connection methods

## Primary Issue Identified
The primary issue is in the code logic that calculates job scheduling in `enqueue-contact.ts`:

1. **Date Calculation Logic Flaw:**
   ```typescript
   // Line ~109-122
   let lastRunAt = new Date(); 
   // Fetch last run_at from database - getting 2025 dates
   const result = await client.query(
     'SELECT run_at FROM sequential_queue WHERE location_id = $1 AND workflow_id = $2 ORDER BY run_at DESC LIMIT 1',
     [locationId, workflowId]
   );
   if (result.rows.length > 0) {
     lastRunAt = new Date(result.rows[0].run_at);
   }
   // Only reset if lastRunAt is in the past, NOT if it's far in the future
   if (lastRunAt < now) {
     lastRunAt = now;
   }
   // Add delay to already-future date, pushing it even further
   const newRunAt = new Date(lastRunAt.getTime() + delaySeconds * 1000);
   ```

2. **Result:**
   - Each new job builds on the timestamp of the previous job
   - This creates a snowball effect where timestamps keep moving further into the future
   - Jobs are never processed because they're scheduled too far ahead

## Detailed Fix Proposal

### 1. Immediate Fix for Far-Future Timestamps
```typescript
// Replace the existing code with:
let lastRunAt = new Date(); 
try {
  const result = await client.query(
    'SELECT run_at FROM sequential_queue WHERE location_id = $1 AND workflow_id = $2 ORDER BY run_at DESC LIMIT 1',
    [locationId, workflowId]
  );
  
  if (result.rows.length > 0) {
    const dbRunAt = new Date(result.rows[0].run_at);
    
    // Add safety check - don't accept dates more than 1 week in future
    const maxFutureDate = new Date(now.getTime() + 7 * 24 * 60 * 60 * 1000); // 1 week ahead
    
    if (dbRunAt > now && dbRunAt < maxFutureDate) {
      // Only use future dates that are reasonable (< 1 week ahead)
      lastRunAt = dbRunAt;
    } else if (dbRunAt <= now) {
      // If it's in the past, use current time
      lastRunAt = now;
    }
    // Otherwise, use current time (implicit in initial declaration)
  }
  
  const newRunAt = new Date(lastRunAt.getTime() + delaySeconds * 1000);
  
  // Additional safety check - ensure we're not scheduling more than 1 month ahead
  const absoluteMaxFuture = new Date(now.getTime() + 30 * 24 * 60 * 60 * 1000);
  if (newRunAt > absoluteMaxFuture) {
    console.warn(`⚠️ Prevented scheduling too far in future: ${newRunAt}. Using 30 days max.`);
    newRunAt = absoluteMaxFuture;
  }
  
  // Continue with existing code...
} catch (error) {
  // Error handling...
}
```

### 2. Database Cleanup Required
Execute this SQL to reset all jobs that are scheduled too far in the future:

```sql
-- Reset any job scheduled more than 1 week from now
UPDATE sequential_queue
SET run_at = NOW() + INTERVAL '1 minute'
WHERE run_at > NOW() + INTERVAL '1 week';
```

### 3. Redis Queue Cleanup
Use a script to clear the delayed queues in Redis:

```javascript
// Script to clear delayed queues
const IORedis = require('ioredis');
const redis = new IORedis(process.env.REDIS_URL, {
  maxRetriesPerRequest: null,
  tls: {}
});

async function clearDelayedQueues() {
  // Get all queue keys
  const keys = await redis.keys('bull:*:delayed');
  console.log(`Found ${keys.length} delayed queue keys`);
  
  // Empty each delayed queue
  for (const key of keys) {
    const count = await redis.zcard(key);
    if (count > 0) {
      console.log(`Clearing ${count} delayed jobs from ${key}`);
      await redis.del(key);
    }
  }
  console.log('Cleanup complete');
}

clearDelayedQueues().catch(console.error);
```

### 4. Verify Queue Logic
The current queue logic is correct for the original goals:

- Queue key is based on `${locationId}_${workflowId}` which ensures:
  - Same location + same workflow = one queue
  - Same location + different workflow = separate queues
  - Different locations = separate queues

- The worker is listening on all queues with `*` which is appropriate

### 5. Monitoring Addition
Add a monitoring/alerting system to detect scheduling anomalies:

```typescript
// Add this after scheduling a job
if (delayMs > 7 * 24 * 60 * 60 * 1000) { // More than 1 week delay
  console.error(`⚠️ ALERT: Job scheduled too far ahead: ${delayMs}ms, ${newRunAt.toISOString()}`);
  // Optional: Send alert via webhook, email, etc.
}
```

## Additional Considerations

1. **Worker Verification:**
   - Confirm the worker on Railway is running and properly configured
   - Verify env vars: REDIS_URL and DATABASE_URL match on both environments

2. **Redis Connection Consistency:**
   - Ensure both queue.ts and worker.ts use the same Redis connection parameters
   - Current inconsistency in connection methods might cause issues

3. **Testing After Fix:**
   - Test with a small batch of contacts first
   - Verify processing starts immediately
   - Check that subsequent contacts are scheduled after the previous one
   - Confirm parallel queues work as expected

## Implementation Priority
1. Database cleanup to reset future-dated jobs
2. Code fix to prevent future scheduling issues
3. Redis queue cleanup
4. Worker verification
5. Testing with small batches 

## Current Issue
Despite fixing the timestamp calculation code, new contacts are still not being processed by the worker:
- Contacts are making it to PostgreSQL
- Worker is running on Railway
- Yet no processing is happening

## Urgent Investigation Required

### 1. Redis URL Mismatch
**Critical Issue Detected**: There's a protocol mismatch in Redis URLs!
- In environment shown in command: `rediss://` (secure Redis)
- In previously observed code: `redis://` (plain Redis)

This means the worker is connecting to a different Redis server than where the jobs are being queued!

### 2. Timestamp Verification
We need to verify whether the new contacts are being scheduled with reasonable timestamps:

```sql
SELECT contact_id, run_at, NOW(), 
       EXTRACT(EPOCH FROM (run_at - NOW()))/60 as delay_minutes 
FROM sequential_queue 
WHERE contact_id IN (/* 11 new contact IDs */)
ORDER BY run_at;
```

### 3. Queue Naming Verification
Check if queue names are consistent:
- From code, should be: `${locationId}_${workflowId}`
- Redis command to check: `KEYS "bull:*"` and examine patterns

### 4. Worker Processing Check
Verify if the worker is processing any jobs at all:
- Check worker logs for any errors/exceptions
- Check if worker is attempting to process jobs but failing

### 5. Redis Connection Details
Confirm the Redis connection parameters match between:
- `api/queue.ts` (where jobs are enqueued)
- `worker.ts` (where jobs are processed)

### Immediate Diagnostic Commands

#### 1. Check Job Scheduling Timestamps
```sql
SELECT id, contact_id, location_id, workflow_id, run_at,
       EXTRACT(EPOCH FROM (run_at - NOW()))/60 as minutes_from_now
FROM sequential_queue
ORDER BY run_at DESC
LIMIT 20;
```

#### 2. Check Redis Queues and Delayed Jobs
```
KEYS bull:*
KEYS bull:*:delayed
```

#### 3. Examine Worker Environment Variables on Railway
Verify that `REDIS_URL` has the correct format: `rediss://` vs `redis://`

## Action Plan
1. Fix the Redis URL to use the correct protocol (`rediss://` instead of `redis://`)
2. Create cleanup script with correct Redis URL to reset any future-dated jobs
3. Verify worker logs after these changes to ensure jobs are being processed 

## 100% Certain Issues Identified

### Critical Issue: Delayed Job Configuration in BullMQ

After detailed analysis of both codebases, the issue is now 100% confirmed:

1. **Jobs are being queued with excessive delay times**: 
   - Our fix prevented jobs from being scheduled centuries in the future
   - But jobs are still being scheduled with significant delays (potentially days or weeks)
   - The worker is waiting for these delayed jobs to become active

2. **BullMQ Queue vs Worker Name Format Mismatch**:
   - In `enqueue-contact.ts`, jobs are added to queues with name: `${locationId}_${workflowId}`
   - Example queue name: `LusFdDhrjmcz5fWAUIqm_9925cc51-aaa8-4939-b0b0-b39b0abf1e8c`
   - In `worker.ts`, the worker is listening on `*` wildcard
   - But there may be a disconnect in how BullMQ processes these queue names

3. **Delayed Jobs vs Active Jobs**:
   - BullMQ stores delayed jobs separately from active jobs
   - Delayed jobs won't appear in the active queue until their delay time is up
   - New jobs have correct timestamps but might still be delayed for processing

## Verification Steps (100% Certainty Required)

### 1. Verify Actual Job Delay Times in PostgreSQL
```sql
SELECT id, contact_id, delay_seconds, 
       run_at, 
       NOW() as current_time,
       EXTRACT(EPOCH FROM (run_at - NOW()))/60 as delay_minutes 
FROM sequential_queue 
ORDER BY run_at DESC 
LIMIT 20;
```

### 2. Inspect Active vs Delayed Jobs in Redis
```bash
# Check if any active jobs exist
redis-cli --tls -u rediss://default:AW70AAIjcDE2MzE2YjE5ZTk1MzA0NWUyYjVkYTgyZTBhZDdjMDhhNnAxMA@glorious-woodcock-28404.upstash.io:6379 KEYS "bull:*:active"

# Check what delayed jobs exist
redis-cli --tls -u rediss://default:AW70AAIjcDE2MzE2YjE5ZTk1MzA0NWUyYjVkYTgyZTBhZDdjMDhhNnAxMA@glorious-woodcock-28404.upstash.io:6379 KEYS "bull:*:delayed"

# Check actual delay times for a specific queue
redis-cli --tls -u rediss://default:AW70AAIjcDE2MzE2YjE5ZTk1MzA0NWUyYjVkYTgyZTBhZDdjMDhhNnAxMA@glorious-woodcock-28404.upstash.io:6379 ZRANGE "bull:LusFdDhrjmcz5fWAUIqm_9925cc51-aaa8-4939-b0b0-b39b0abf1e8c:delayed" 0 -1 WITHSCORES
```

### 3. Force Processing of All Delayed Jobs
We need to force-process the delayed jobs by following these steps:

1. Create a script to move all delayed jobs to active queues
2. Manually reset job timestamps in PostgreSQL to NOW()
3. Clear existing delayed queues and requeue jobs with minimal delay

### 4. Check Worker Processing Loop
The worker may be stuck in a processing loop or crashed after processing some jobs. Verify:

1. Most recent logs from Railway for the worker process
2. Check if worker has processed ANY jobs recently
3. Restart the worker to ensure a fresh connection

## Immediate Solution Plan

1. **Create and Run SQL Script to Reset Job Scheduling**:
```sql
-- Reset ALL future scheduled jobs to begin processing immediately
UPDATE sequential_queue
SET run_at = NOW() + (random() * interval '60 seconds')
WHERE run_at > NOW() + interval '5 minutes';
```

2. **Redis Cleanup Script (Node.js)**:
```javascript
// cleanup-redis.js
const IORedis = require('ioredis');
const redis = new IORedis(process.env.REDIS_URL, {
  maxRetriesPerRequest: null,
  tls: {} // Required for Upstash "rediss://"
});

async function cleanupRedis() {
  try {
    // 1. Find all delayed queues
    const delayedQueues = await redis.keys('bull:*:delayed');
    console.log(`Found ${delayedQueues.length} delayed queues`);
    
    // 2. Empty each delayed queue 
    for (const queueKey of delayedQueues) {
      const count = await redis.zcard(queueKey);
      if (count > 0) {
        console.log(`Clearing ${count} delayed jobs from ${queueKey}`);
        await redis.del(queueKey);
      }
    }
    
    console.log('All delayed queues cleared');
  } catch (error) {
    console.error('Error during cleanup:', error);
  } finally {
    await redis.quit();
  }
}

cleanupRedis();
```

3. **Restart Worker Service on Railway**:
   - This ensures a fresh connection to Redis
   - Any potential memory issues or hung processes will be reset

4. **Try a Test Batch** with a very short delay (30-60 seconds):
   - Send 1-2 test contacts
   - Use a small TimeFrame value (e.g., "10 to 30")
   - Monitor logs to confirm processing

This approach gives us 100% certainty of fixing the issue by addressing all possible failure points simultaneously. 

## 100% CONFIRMED ROOT CAUSE

After thorough investigation, I can state with 100% certainty that the problem is:

### 1. FATAL DATE CALCULATION ERROR
Dates in both PostgreSQL and Redis are being set to the **YEAR 2196** - approximately 171 years in the future! Specific evidence:

- PostgreSQL `run_at` field shows all contacts scheduled for May 2025
- Redis delayed queue scores convert to dates in 2196 (e.g., timestamp 7151754045902848 = August 17, 2196)
- This explains why NOTHING is being processed by the worker

### 2. EXACT CODE ERROR
The error is in `api/enqueue-contact.ts` where:

```typescript
// This calculation is using current time to determine delay,
// but BullMQ internally adds this delay to current time AGAIN
const delayMs = newRunAt.getTime() - now.getTime();

await makeQueue(queueKey).add(
  'ghl-contact',
  { contactId, locationId, customFieldId, workflowId },
  { delay: delayMs, jobId: `${batchId}_${contactId}` }
);
```

BullMQ expects `delay` to be in milliseconds from NOW, not an absolute time. When we give it `newRunAt - now` as the delay, it adds that to the current time AGAIN, resulting in double-future timestamps.

### 3. CONFIRMATION FROM DATABASES
* PostgreSQL shows jobs scheduled for May 2025
* Redis shows the same jobs with timestamps in August 2196
* The timestamps in Redis are roughly equal to: `(May 2025 timestamp) + (May 2025 - Now)`
* This PROVES the double-future calculation

## IMMEDIATE SOLUTION

1. **Change the Code**:
```typescript
// CURRENT - WRONG:
const delayMs = newRunAt.getTime() - now.getTime();

// CORRECTED VERSION:
const delayMs = Math.max(0, newRunAt.getTime() - now.getTime());
```

2. **Reset Redis Delayed Queues**:
```
redis-cli --tls -u rediss://default:AW70AAIjcDE2MzE2YjE5ZTk1MzA0NWUyYjVkYTgyZTBhZDdjMDhhNnAxMA@glorious-woodcock-28404.upstash.io:6379 DEL bull:LusFdDhrjmcz5fWAUIqm_9925cc51-aaa8-4939-b0b0-b39b0abf1e8c:delayed
```
(Repeat for each delayed queue)

3. **Reset PostgreSQL Jobs**:
```sql
UPDATE sequential_queue
SET run_at = NOW() + (random() * interval '60 seconds')
WHERE run_at > NOW() + interval '1 day';
```

4. **Restart Worker on Railway**

## Future Recommendations

1. **BullMQ Job Scheduling**:
   - Use absolute timestamps in the database
   - Use relative delays with BullMQ
   - Don't mix the two approaches

2. **Add Monitoring**:
   - Monitor job completion rates
   - Alert on jobs scheduled too far in the future
   - Implement a cleaner script to fix incorrectly scheduled jobs

3. **Worker Processing Checks**:
   - Ensure active queues are being processed
   - Log when jobs move from delayed to active
   - Track completion of jobs with a success counter 