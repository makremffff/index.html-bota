// /api/index.js (Final and Secure Version with Limit-Based Reset)

/**
 * SHIB Ads WebApp Backend API
 * Handles all POST requests from the Telegram Mini App frontend.
 * Uses the Supabase REST API for persistence.
 */
const crypto = require('crypto');

// Load environment variables for Supabase connection
const SUPABASE_URL = process.env.NEXT_PUBLIC_SUPABASE_URL;
const SUPABASE_ANON_KEY = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY;
// ⚠️ BOT_TOKEN must be set in Vercel environment variables
const BOT_TOKEN = process.env.BOT_TOKEN;

// ------------------------------------------------------------------
// Fully secured and defined server-side constants
// ------------------------------------------------------------------
const REWARD_PER_AD = 3;
const REFERRAL_COMMISSION_RATE = 0.05;
const DAILY_MAX_ADS = 100; // Max ads limit
const DAILY_MAX_SPINS = 15; // Max spins limit
const RESET_INTERVAL_MS = 6 * 60 * 60 * 1000; // ⬅️ 6 hours in milliseconds
const MIN_TIME_BETWEEN_ACTIONS_MS = 3000; // 3 seconds minimum time between watchAd/spin requests
const ACTION_ID_EXPIRY_MS = 60000; // 60 seconds for Action ID to be valid
const SPIN_SECTORS = [5, 10, 15, 20, 5];

// ------------------------------------------------------------------
// NEW Task Constants
// ------------------------------------------------------------------
const TASK_REWARD = 50;
const TELEGRAM_CHANNEL_USERNAME = '@botbababab'; // يجب أن يكون هذا هو اسم المستخدم للقناة لبدء التحقق


/**
 * Helper function to randomly select a prize from the defined sectors and return its index.
 */
function calculateRandomSpinPrize() {
    const randomIndex = Math.floor(Math.random() * SPIN_SECTORS.length);
    const prize = SPIN_SECTORS[randomIndex];
    return { prize, prizeIndex: randomIndex };
}

// --- Helper Functions ---

function sendSuccess(res, data = {}) {
  res.writeHead(200, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({ ok: true, data }));
}

function sendError(res, message, statusCode = 400) {
  res.writeHead(statusCode, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({ ok: false, error: message }));
}

async function supabaseFetch(tableName, method, body = null, queryParams = '?select=*') {
  if (!SUPABASE_URL || !SUPABASE_ANON_KEY) {
    throw new Error('Supabase environment variables are not configured.');
  }

  const url = `${SUPABASE_URL}/rest/v1/${tableName}${queryParams}`;

  const headers = {
    'apikey': SUPABASE_ANON_KEY,
    'Authorization': `Bearer ${SUPABASE_ANON_KEY}`,
    'Content-Type': 'application/json',
    'Prefer': 'return=representation'
  };

  const options = {
    method,
    headers,
    body: body ? JSON.stringify(body) : null,
  };

  const response = await fetch(url, options);

  if (response.ok) {
      const responseText = await response.text();
      try {
          const jsonResponse = JSON.parse(responseText);
          return Array.isArray(jsonResponse) ? jsonResponse : { success: true };
      } catch (e) {
          return { success: true };
      }
  }

  let data;
  try {
      data = await response.json();
  } catch (e) {
      const errorMsg = `Supabase error: ${response.status} ${response.statusText}`;
      throw new Error(errorMsg);
  }

  const errorMsg = data.message || `Supabase error: ${response.status} ${response.statusText}`;
  throw new Error(errorMsg);
}

/**
 * Checks if a user is a member (or creator/admin) of a specific Telegram channel.
 */
async function checkChannelMembership(userId, channelUsername) {
    if (!BOT_TOKEN) {
        console.error('BOT_TOKEN is not configured for membership check.');
        return false;
    }
    
    // The chat_id must be in the format @username or -100xxxxxxxxxx
    const chatId = channelUsername.startsWith('@') ? channelUsername : `@${channelUsername}`; 

    const url = `https://api.telegram.org/bot${BOT_TOKEN}/getChatMember?chat_id=${chatId}&user_id=${userId}`;
    
    try {
        const response = await fetch(url);
        if (!response.ok) {
            const errorData = await response.json();
            console.error('Telegram API error (getChatMember):', errorData.description || response.statusText);
            return false;
        }

        const data = await response.json();
        
        if (!data.ok) {
             console.error('Telegram API error (getChatMember - not ok):', data.description);
             return false;
        }

        const status = data.result.status;
        
        // Accepted statuses are 'member', 'administrator', 'creator'
        const isMember = ['member', 'administrator', 'creator'].includes(status);
        
        return isMember;

    } catch (error) {
        console.error('Network or parsing error during Telegram API call:', error.message);
        return false;
    }
}


/**
 * Limit-Based Reset Logic: Resets counters if the limit was reached AND the interval (6 hours) has passed since.
 * ⚠️ هذا هو التعديل الرئيسي: يعتمد على أعمدة الوصول للحد الأقصى وليس على آخر نشاط عام.
 */
async function resetDailyLimitsIfExpired(userId) {
    const now = Date.now();

    try {
        // 1. Fetch current limits and the time they were reached
        const users = await supabaseFetch('users', 'GET', null, `?id=eq.${userId}&select=ads_watched_today,spins_today,ads_limit_reached_at,spins_limit_reached_at`);
        if (!Array.isArray(users) || users.length === 0) {
            return;
        }

        const user = users[0];
        const updatePayload = {};

        // 2. Check Ads Limit Reset
        if (user.ads_limit_reached_at && user.ads_watched_today >= DAILY_MAX_ADS) {
            const adsLimitTime = new Date(user.ads_limit_reached_at).getTime();
            if (now - adsLimitTime > RESET_INTERVAL_MS) {
                // ⚠️ تم مرور 6 ساعات على الوصول للحد الأقصى، يتم إعادة التعيين
                updatePayload.ads_watched_today = 0;
                updatePayload.ads_limit_reached_at = null; // إزالة الوقت لانتهاء فترة القفل
                console.log(`Ads limit reset for user ${userId}.`);
            }
        }

        // 3. Check Spins Limit Reset
        if (user.spins_limit_reached_at && user.spins_today >= DAILY_MAX_SPINS) {
            const spinsLimitTime = new Date(user.spins_limit_reached_at).getTime();
            if (now - spinsLimitTime > RESET_INTERVAL_MS) {
                // ⚠️ تم مرور 6 ساعات على الوصول للحد الأقصى، يتم إعادة التعيين
                updatePayload.spins_today = 0;
                updatePayload.spins_limit_reached_at = null; // إزالة الوقت لانتهاء فترة القفل
                console.log(`Spins limit reset for user ${userId}.`);
            }
        }

        // 4. Perform the database update if any limits were reset
        if (Object.keys(updatePayload).length > 0) {
            await supabaseFetch('users', 'PATCH',
                updatePayload,
                `?id=eq.${userId}`);
        }
    } catch (error) {
        console.error(`Failed to check/reset daily limits for user ${userId}:`, error.message);
    }
}

/**
 * Rate Limiting Check for Ad/Spin Actions
 * ⚠️ تم تعديلها: لم تعد تحدث last_activity، بل فقط تفحص الفارق الزمني الأخير
 */
async function checkRateLimit(userId) {
    try {
        const users = await supabaseFetch('users', 'GET', null, `?id=eq.${userId}&select=last_activity`);
        if (!Array.isArray(users) || users.length === 0) {
            return { ok: true };
        }

        const user = users[0];
        // إذا كان last_activity غير موجود، يمكن اعتباره 0 لضمان السماح بالمرور
        const lastActivity = user.last_activity ? new Date(user.last_activity).getTime() : 0; 
        const now = Date.now();
        const timeElapsed = now - lastActivity;

        if (timeElapsed < MIN_TIME_BETWEEN_ACTIONS_MS) {
            const remainingTime = MIN_TIME_BETWEEN_ACTIONS_MS - timeElapsed;
            return {
                ok: false,
                message: `Rate limit exceeded. Please wait ${Math.ceil(remainingTime / 1000)} seconds before the next action.`,
                remainingTime: remainingTime
            };
        }
        // تحديث last_activity سيتم لاحقاً في دوال watchAd/spinResult/task
        return { ok: true };
    } catch (error) {
        console.error(`Rate limit check failed for user ${userId}:`, error.message);
        return { ok: true };
    }
}

// ------------------------------------------------------------------
// **initData Security Validation Function** (No change)
// ------------------------------------------------------------------
function validateInitData(initData) {
    if (!initData || !BOT_TOKEN) {
        console.warn('Security Check Failed: initData or BOT_TOKEN is missing.');
        return false;
    }

    const urlParams = new URLSearchParams(initData);
    const hash = urlParams.get('hash');
    urlParams.delete('hash');

    const dataCheckString = Array.from(urlParams.entries())
        .map(([key, value]) => `${key}=${value}`)
        .sort()
        .join('\n');

    const secretKey = crypto.createHmac('sha256', 'WebAppData')
        .update(BOT_TOKEN)
        .digest();

    const calculatedHash = crypto.createHmac('sha256', secretKey)
        .update(dataCheckString)
        .digest('hex');

    if (calculatedHash !== hash) {
        console.warn(`Security Check Failed: Hash mismatch.`);
        return false;
    }

    const authDateParam = urlParams.get('auth_date');
    if (!authDateParam) {
        console.warn('Security Check Failed: auth_date is missing.');
        return false;
    }

    const authDate = parseInt(authDateParam) * 1000;
    const currentTime = Date.now();
    const expirationTime = 1200 * 1000; // 20 minutes limit

    if (currentTime - authDate > expirationTime) {
        console.warn(`Security Check Failed: Data expired.`);
        return false;
    }

    return true;
}

// ------------------------------------------------------------------
// 🔑 Commission Helper Function (No change)
// ------------------------------------------------------------------
/**
 * Processes the commission for the referrer and updates their balance.
 */
async function processCommission(referrerId, refereeId, sourceReward) {
    // 1. Calculate commission
    const commissionAmount = sourceReward * REFERRAL_COMMISSION_RATE; 
    
    if (commissionAmount < 0.000001) { 
        console.log(`Commission too small (${commissionAmount}). Aborted for referee ${refereeId}.`);
        return { ok: false, error: 'Commission amount is effectively zero.' };
    }

    try {
        // 2. Fetch referrer's current balance and status
        const users = await supabaseFetch('users', 'GET', null, `?id=eq.${referrerId}&select=balance,is_banned`);
        if (!Array.isArray(users) || users.length === 0 || users[0].is_banned) {
            console.log(`Referrer ${referrerId} not found or banned. Commission aborted.`);
            return { ok: false, error: 'Referrer not found or banned, commission aborted.' };
        }

        // 3. Update balance: newBalance will now include the decimal commission
        const newBalance = users[0].balance + commissionAmount;
        
        // 4. Update referrer balance
        await supabaseFetch('users', 'PATCH', { balance: newBalance }, `?id=eq.${referrerId}`);

        // 5. Add record to commission_history
        await supabaseFetch('commission_history', 'POST', {
            referrer_id: referrerId,
            referee_id: refereeId,
            amount: commissionAmount,
            source_reward: sourceReward
        }, '?select=referrer_id');

        return { ok: true, commission: commissionAmount };

    } catch (error) {
        console.error(`ProcessCommission failed for referrer ${referrerId}:`, error.message);
        return { ok: false, error: error.message };
    }
}

// ------------------------------------------------------------------
// Action ID Security Functions (No change)
// ------------------------------------------------------------------

/**
 * Generates a unique action ID and stores it in the database.
 */
async function generateAndSaveActionId(res, userId, actionType) {
    const actionId = crypto.randomBytes(32).toString('hex');
    try {
        await supabaseFetch('temp_actions', 'POST', {
            user_id: userId,
            action_id: actionId,
            action_type: actionType
        }, '?select=id');
        return actionId;
    } catch (error) {
        console.error('Failed to generate and save action ID:', error.message);
        sendError(res, 'Failed to generate security token.', 500);
        return null;
    }
}

/**
 * Middleware: Checks if the Action ID is valid and then deletes it.
 */
async function validateAndUseActionId(res, userId, actionId, actionType) {
    if (!actionId) {
        sendError(res, 'Missing Server Token (Action ID). Request rejected.', 400);
        return false;
    }

    try {
        const query = `?user_id=eq.${userId}&action_id=eq.${actionId}&action_type=eq.${actionType}&select=id,created_at`;
        const records = await supabaseFetch('temp_actions', 'GET', null, query);

        if (!Array.isArray(records) || records.length === 0) {
            sendError(res, 'Invalid or previously used Server Token (Action ID).', 409);
            return false;
        }

        const record = records[0];
        const recordTime = new Date(record.created_at).getTime();

        // 1. Check Expiration (60 seconds)
        if (Date.now() - recordTime > ACTION_ID_EXPIRY_MS) {
            // Delete the expired token to clean up
            await supabaseFetch('temp_actions', 'DELETE', null, `?id=eq.${record.id}`);
            sendError(res, 'Server Token (Action ID) expired. Please try again.', 408);
            return false;
        }

        // 2. Use (Delete) the token now
        await supabaseFetch('temp_actions', 'DELETE', null, `?id=eq.${record.id}`);

        return true; // Token is valid and has been used
    } catch (error) {
        console.error('Action ID validation failed:', error.message);
        sendError(res, `Security token validation failed: ${error.message}`, 500);
        return false;
    }
}

// ------------------------------------------------------------------
// **API Handlers** (Original Handlers - No change)
// ------------------------------------------------------------------

/**
 * 0) type: "generateActionId" (No change)
 */
async function handleGenerateActionId(req, res, body) {
    const { user_id, action_type } = body;
    const id = parseInt(user_id);

    try {
        const actionId = await generateAndSaveActionId(res, id, action_type);
        if (actionId) {
            sendSuccess(res, { action_id: actionId });
        }
    } catch (error) {
        sendError(res, 'Failed to generate security token.', 500);
    }
}

/**
 * 1) type: "getUserData" (No change)
 */
async function handleGetUserData(req, res, body) {
    const { user_id } = body;
    const id = parseInt(user_id);
    try {
        // 1. Check/Reset Daily Limits before fetching data
        await resetDailyLimitsIfExpired(id);

        // 2. Fetch User Data, Withdrawal History, and Referral Count
        const users = await supabaseFetch('users', 'GET', null, `?id=eq.${id}&select=*,referrer:users!ref_by(id,balance,is_banned)`);
        if (!Array.isArray(users) || users.length === 0) {
            return sendError(res, 'User not found. Please register first.', 404);
        }
        const userData = users[0];

        // Fetch withdrawal history
        const withdrawalHistory = await supabaseFetch('withdrawal_history', 'GET', null, `?user_id=eq.${id}&select=amount,status,created_at&order=created_at.desc`);

        // Fetch referral count
        const referralsCountData = await supabaseFetch('users', 'GET', null, `?ref_by=eq.${id}&select=count`);
        const referralsCount = referralsCountData.length > 0 ? referralsCountData[0].count : 0;

        // 3. Update last_activity (not for rate limit, but for general tracking/user presence now)
        await supabaseFetch('users', 'PATCH', { last_activity: new Date().toISOString() }, `?id=eq.${id}&select=id`);

        sendSuccess(res, { ...userData, referrals_count: referralsCount, withdrawal_history: withdrawalHistory });
    } catch (error) {
        console.error('GetUserData failed:', error.message);
        sendError(res, `Failed to retrieve user data: ${error.message}`, 500);
    }
}

/**
 * 2) type: "register" (No change)
 */
async function handleRegister(req, res, body) {
    const { user_id, ref_by } = body;
    const id = parseInt(user_id);
    try {
        // 1. Check if user exists
        const users = await supabaseFetch('users', 'GET', null, `?id=eq.${id}&select=id,is_banned`);
        
        if (!Array.isArray(users) || users.length === 0) {
            // 2. User does not exist, create new user
            const newUser = {
                id,
                balance: 0,
                ads_watched_today: 0,
                spins_today: 0,
                task_completed: false, // ⬅️ NEW: Initialize task_completed field
                ref_by: ref_by ? parseInt(ref_by) : null,
                last_activity: new Date().toISOString()
            };
            await supabaseFetch('users', 'POST', newUser, '?select=id');
            sendSuccess(res, { message: 'User registered successfully.' });
        } else if (users[0].is_banned) {
            sendError(res, 'User is banned.', 403);
        } else {
            // 3. User exists
            sendSuccess(res, { message: 'User already registered.' });
        }

    } catch (error) {
        console.error('Register failed:', error.message);
        sendError(res, `Failed to register user: ${error.message}`, 500);
    }
}

/**
 * 3) type: "watchAd" (No change)
 */
async function handleWatchAd(req, res, body) {
    const { user_id, action_id } = body;
    const id = parseInt(user_id);

    // 1. Validate Action ID
    if (!await validateAndUseActionId(res, id, action_id, 'watchAd')) {
        return;
    }

    try {
        // 2. Fetch User Data
        const users = await supabaseFetch('users', 'GET', null, `?id=eq.${id}&select=id,balance,is_banned,ads_watched_today,ads_limit_reached_at,ref_by`);
        if (!Array.isArray(users) || users.length === 0) {
            return sendError(res, 'User not found.', 404);
        }
        const user = users[0];

        // 3. Banned Check
        if (user.is_banned) {
            return sendError(res, 'User is banned.', 403);
        }

        // 4. Rate Limit Check
        const rateLimitResult = await checkRateLimit(id);
        if (!rateLimitResult.ok) {
            return sendError(res, rateLimitResult.message, 429);
        }

        // 5. Check maximum ads limit
        if (user.ads_watched_today >= DAILY_MAX_ADS) {
            // This case should be rare if resetDailyLimitsIfExpired runs correctly, but serves as a final safety
            return sendError(res, `Daily ad limit (${DAILY_MAX_ADS}) reached.`, 403);
        }

        // --- All checks passed: Process Ad Reward ---
        const reward = REWARD_PER_AD;
        const referrerId = user.ref_by;
        const newBalance = user.balance + reward;
        const newAdsCount = user.ads_watched_today + 1;

        const updatePayload = {
            balance: newBalance,
            ads_watched_today: newAdsCount,
            last_activity: new Date().toISOString() // ⬅️ تحديث لـ Rate Limit
        };

        // 8. ⚠️ NEW LOGIC: Check if the limit is reached NOW
        if (newAdsCount >= DAILY_MAX_ADS) {
            updatePayload.ads_limit_reached_at = new Date().toISOString();
        }

        // 9. Update user record
        await supabaseFetch('users', 'PATCH', updatePayload, `?id=eq.${id}`);

        // 10. Commission Call
        if (referrerId) {
            processCommission(referrerId, id, reward).catch(e => {
                console.error(`WatchAd Commission failed silently for referrer ${referrerId}:`, e.message);
            });
        }

        // 11. Success
        sendSuccess(res, { new_balance: newBalance, actual_reward: reward, new_ads_count: newAdsCount });

    } catch (error) {
        console.error('WatchAd failed:', error.message);
        sendError(res, `Failed to process ad watch: ${error.message}`, 500);
    }
}

/**
 * 4) type: "preSpin" (No change)
 */
async function handlePreSpin(req, res, body) {
    const { user_id, action_id } = body;
    const id = parseInt(user_id);

    // 1. Validate Action ID (Check only, we don't use it yet)
    if (!await validateAndUseActionId(res, id, action_id, 'preSpin')) {
        return;
    }

    try {
        // 2. Fetch User Data
        const users = await supabaseFetch('users', 'GET', null, `?id=eq.${id}&select=is_banned,spins_today,spins_limit_reached_at`);
        if (!Array.isArray(users) || users.length === 0) {
            return sendError(res, 'User not found.', 404);
        }
        const user = users[0];

        // 3. Banned Check
        if (user.is_banned) {
            return sendError(res, 'User is banned.', 403);
        }

        // 4. Rate Limit Check (before proceeding with spin)
        const rateLimitResult = await checkRateLimit(id);
        if (!rateLimitResult.ok) {
            return sendError(res, rateLimitResult.message, 429);
        }

        // 5. Check maximum spin limit
        if (user.spins_today >= DAILY_MAX_SPINS) {
            return sendError(res, `Daily spin limit (${DAILY_MAX_SPINS}) reached.`, 403);
        }

        // 6. Success: Ready to spin
        sendSuccess(res, { message: 'Pre-spin check passed.' });

    } catch (error) {
        console.error('PreSpin failed:', error.message);
        sendError(res, `Failed to prepare for spin: ${error.message}`, 500);
    }
}

/**
 * 5) type: "spinResult" (No change)
 */
async function handleSpinResult(req, res, body) {
    const { user_id, action_id } = body;
    const id = parseInt(user_id);

    // 1. Validate Action ID (for spinResult)
    if (!await validateAndUseActionId(res, id, action_id, 'spinResult')) {
        return;
    }

    try {
        // 2. Fetch User Data
        const users = await supabaseFetch('users', 'GET', null, `?id=eq.${id}&select=id,balance,is_banned,spins_today,spins_limit_reached_at,ref_by`);
        if (!Array.isArray(users) || users.length === 0) {
            return sendError(res, 'User not found.', 404);
        }
        const user = users[0];

        // 3. Banned Check
        if (user.is_banned) {
            return sendError(res, 'User is banned.', 403);
        }

        // 4. Rate Limit Check
        const rateLimitResult = await checkRateLimit(id);
        if (!rateLimitResult.ok) {
            return sendError(res, rateLimitResult.message, 429);
        }

        // 5. Check maximum spin limit
        if (user.spins_today >= DAILY_MAX_SPINS) {
            return sendError(res, `Daily spin limit (${DAILY_MAX_SPINS}) reached.`, 403);
        }

        // --- All checks passed: Process Spin Result ---
        const { prize, prizeIndex } = calculateRandomSpinPrize();
        const newSpinsCount = user.spins_today + 1;
        const newBalance = user.balance + prize;

        const updatePayload = {
            balance: newBalance,
            spins_today: newSpinsCount,
            last_activity: new Date().toISOString() // ⬅️ تحديث لـ Rate Limit
        };

        // 7. ⚠️ NEW LOGIC: Check if the limit is reached NOW
        if (newSpinsCount >= DAILY_MAX_SPINS) {
            updatePayload.spins_limit_reached_at = new Date().toISOString();
        }

        // 8. Update user record
        await supabaseFetch('users', 'PATCH', updatePayload, `?id=eq.${id}`);

        // 9. Commission Call
        const referrerId = user.ref_by;
        if (referrerId) {
             processCommission(referrerId, id, prize).catch(e => {
                 console.error(`Spin Commission failed silently for referrer ${referrerId}:`, e.message);
             });
        }

        // 10. Success
        sendSuccess(res, { new_balance: newBalance, actual_reward: prize, new_spins_count: newSpinsCount, prize_index: prizeIndex });

    } catch (error) {
        console.error('SpinResult failed:', error.message);
        sendError(res, `Failed to process spin result: ${error.message}`, 500);
    }
}

/**
 * 6) type: "completeTask" (Original Channel Join Task - No change)
 */
async function handleCompleteTask(req, res, body) {
    const { user_id, action_id } = body;
    const id = parseInt(user_id);
    const reward = TASK_REWARD;

    // 1. Validate Action ID
    if (!await validateAndUseActionId(res, id, action_id, 'completeTask')) {
        return;
    }

    try {
        // 2. Fetch User Data
        const users = await supabaseFetch('users', 'GET', null, `?id=eq.${id}&select=id,balance,is_banned,task_completed`);
        if (!Array.isArray(users) || users.length === 0) {
            return sendError(res, 'User not found.', 404);
        }
        const user = users[0];

        // 3. Banned Check
        if (user.is_banned) {
            return sendError(res, 'User is banned.', 403);
        }

        // 4. Check if already completed
        if (user.task_completed) {
            return sendError(res, 'Task already completed.', 409);
        }

        // 5. Check Rate Limit
        const rateLimitResult = await checkRateLimit(id);
        if (!rateLimitResult.ok) {
            return sendError(res, rateLimitResult.message, 429);
        }

        // 6. Check Channel Membership
        const isMember = await checkChannelMembership(id, TELEGRAM_CHANNEL_USERNAME);

        if (!isMember) {
             // ⚠️ هنا نستخدم 409 Conflict للإشارة إلى أن الشرط لم يتحقق
            return sendError(res, 'User is not a member of the required channel.', 409);
        }

        // 7. Process Reward and Update User Data
        const newBalance = user.balance + reward;
        const updatePayload = {
            balance: newBalance,
            task_completed: true, // Mark as completed
            last_activity: new Date().toISOString() // Update for Rate Limit
        };
        await supabaseFetch('users', 'PATCH', updatePayload, `?id=eq.${id}`);

        // 8. Success
        sendSuccess(res, { new_balance: newBalance, actual_reward: reward, message: 'Task completed successfully.' });

    } catch (error) {
        console.error('CompleteTask failed:', error.message);
        sendError(res, `Failed to complete task: ${error.message}`, 500);
    }
}

// ------------------------------------------------------------------
// ⬇️ NEW CODE ADDITION START: NEW Task System API Handlers
// ------------------------------------------------------------------

/**
 * 7) type: "getNewTasks"
 * Retrieves active tasks from tasks_new and checks which ones the user has completed.
 */
async function handleGetNewTasks(req, res, body) {
    const { user_id } = body;
    const id = parseInt(user_id);
    try {
        // 1. Fetch active tasks
        const tasksQuery = `?is_active=eq.true&select=id,task_name,task_url,task_reward,max_users,current_users&order=task_reward.desc`;
        const activeTasks = await supabaseFetch('tasks_new', 'GET', null, tasksQuery);

        // 2. Fetch user's completed tasks
        const userTasksQuery = `?user_id=eq.${id}&select=task_id`;
        const completedTasks = await supabaseFetch('user_tasks_new', 'GET', null, userTasksQuery);
        const completedTaskIds = new Set(completedTasks.map(t => t.task_id));

        // 3. Combine and return
        const tasksWithStatus = activeTasks.map(task => ({
            ...task,
            is_completed: completedTaskIds.has(task.id),
            // Ensure task_reward is treated as a number
            task_reward: parseFloat(task.task_reward) 
        }));

        sendSuccess(res, { tasks: tasksWithStatus });
    } catch (error) {
        console.error('GetNewTasks failed:', error.message);
        sendError(res, `Failed to retrieve new tasks: ${error.message}`, 500);
    }
}


/**
 * 8) type: "completeNewTask"
 * Registers a new task completion, updates user balance, and updates task count.
 * * • التحقق من أن المستخدم لم يكمل المهمة سابقاً.
 * • التحقق من أن current_users < max_users.
 * • إدراج المهمة في user_tasks_new.
 * • تحديث current_users.
 * • إضافة task_reward لرصيد المستخدم عبر النظام الأصلي بدون تعديله.
 */
async function handleCompleteNewTask(req, res, body) {
    const { user_id, task_id, action_id } = body;
    const id = parseInt(user_id);
    const taskId = parseInt(task_id);

    // 1. Validate Action ID
    if (!await validateAndUseActionId(res, id, action_id, 'completeNewTask')) {
        return;
    }

    try {
        // 2. Fetch User Data to get current balance and referrer_id
        const users = await supabaseFetch('users', 'GET', null, `?id=eq.${id}&select=id,balance,is_banned,ref_by`);
        if (!Array.isArray(users) || users.length === 0) {
            return sendError(res, 'User not found.', 404);
        }
        const user = users[0];

        // 3. Banned Check
        if (user.is_banned) {
            return sendError(res, 'User is banned.', 403);
        }

        // 4. Rate Limit Check (before proceeding with complex DB operations)
        const rateLimitResult = await checkRateLimit(id);
        if (!rateLimitResult.ok) {
            return sendError(res, rateLimitResult.message, 429);
        }

        // 5. Fetch Task Details (for reward, max_users, current_users)
        const tasks = await supabaseFetch('tasks_new', 'GET', null, `?id=eq.${taskId}&select=task_reward,max_users,current_users,is_active`);
        if (!Array.isArray(tasks) || tasks.length === 0 || !tasks[0].is_active) {
            return sendError(res, 'Task not found or is no longer active.', 404);
        }
        const task = tasks[0];
        const reward = parseFloat(task.task_reward);

        // 6. Check if user already completed the task (Validation 1)
        const userTasks = await supabaseFetch('user_tasks_new', 'GET', null, `?user_id=eq.${id}&task_id=eq.${taskId}&select=id`);
        if (Array.isArray(userTasks) && userTasks.length > 0) {
            return sendError(res, 'User has already completed this task.', 409); // Conflict
        }

        // 7. Check Max Users Limit (current_users < max_users) (Validation 2)
        if (task.current_users >= task.max_users) {
            return sendError(res, 'Task limit reached. No more slots available.', 408); // Request Timeout/Limit
        }

        // --- All checks passed: Process Task Completion ---

        // 8. Add task completion record (Action 1)
        await supabaseFetch('user_tasks_new', 'POST', {
            user_id: id,
            task_id: taskId
        }, '?select=id');

        // 9. Increment current_users count for the task (Action 2)
        const newCurrentUsers = task.current_users + 1;
        await supabaseFetch('tasks_new', 'PATCH',
            { current_users: newCurrentUsers },
            `?id=eq.${taskId}`);
        
        // 10. Update user balance (Action 3 - using original system's balance logic)
        const newBalance = user.balance + reward;
        const updatePayload = {
            balance: newBalance,
            last_activity: new Date().toISOString() // Update for Rate Limit
        };

        await supabaseFetch('users', 'PATCH', updatePayload, `?id=eq.${id}`);

        // 11. Process Commission (using the original system's helper)
        const referrerId = user.ref_by;
        if (referrerId) {
             processCommission(referrerId, id, reward).catch(e => {
                 console.error(`CompleteNewTask Commission failed silently for referrer ${referrerId}:`, e.message);
             });
        }

        // 12. Success
        sendSuccess(res, {
            new_balance: newBalance,
            actual_reward: reward,
            message: 'New task completed successfully.',
            new_current_users: newCurrentUsers,
        });

    } catch (error) {
        console.error('CompleteNewTask failed:', error.message);
        sendError(res, `Failed to process new task: ${error.message}`, 500);
    }
}
// ⬆️ NEW CODE ADDITION END


/**
 * 9) type: "withdraw" (No change)
 */
async function handleWithdraw(req, res, body) {
    const { user_id, action_id, binanceId, amount } = body;
    const id = parseInt(user_id);
    const withdrawalAmount = parseFloat(amount);

    // 1. Validate Action ID
    if (!await validateAndUseActionId(res, id, action_id, 'withdraw')) {
        return;
    }

    try {
        // 2. Fetch User Data
        const users = await supabaseFetch('users', 'GET', null, `?id=eq.${id}&select=id,balance,is_banned`);
        if (!Array.isArray(users) || users.length === 0) {
            return sendError(res, 'User not found.', 404);
        }
        const user = users[0];

        // 3. Banned Check
        if (user.is_banned) {
            return sendError(res, 'User is banned.', 403);
        }

        // 4. Rate Limit Check
        const rateLimitResult = await checkRateLimit(id);
        if (!rateLimitResult.ok) {
            return sendError(res, rateLimitResult.message, 429);
        }

        // 5. Withdrawal validations (e.g., Min amount, balance check)
        const MIN_WITHDRAW_AMOUNT = 1000;
        if (withdrawalAmount < MIN_WITHDRAW_AMOUNT) {
             return sendError(res, `Minimum withdrawal is ${MIN_WITHDRAW_AMOUNT.toLocaleString()} SHIB.`, 400);
        }
        if (user.balance < withdrawalAmount) {
             return sendError(res, 'Insufficient balance.', 400);
        }

        // 6. Process Withdrawal
        const newBalance = user.balance - withdrawalAmount;
        
        // Transaction: 
        // a. Update balance
        await supabaseFetch('users', 'PATCH', 
            { balance: newBalance, last_activity: new Date().toISOString() }, // Update Rate Limit
            `?id=eq.${id}`);
            
        // b. Insert withdrawal history record
        await supabaseFetch('withdrawal_history', 'POST', 
            { user_id: id, amount: withdrawalAmount, binance_id: binanceId, status: 'Pending' }, 
            '?select=id');
            
        // 7. Success
        sendSuccess(res, { new_balance: newBalance, message: 'Withdrawal request submitted.' });

    } catch (error) {
        console.error('Withdrawal failed:', error.message);
        sendError(res, `Failed to process withdrawal: ${error.message}`, 500);
    }
}


/**
 * 10) type: "commission" (No change)
 */
async function handleCommission(req, res, body) {
    const { referrer_id, referee_id, source_reward } = body;
    const referrerId = parseInt(referrer_id);
    const refereeId = parseInt(referee_id);
    const sourceReward = parseFloat(source_reward);
    
    // This endpoint is for internal server-to-server calls or specific use cases, 
    // so we skip initData validation here, but ensure the inputs are valid.
    if (!referrerId || !refereeId || isNaN(sourceReward) || sourceReward <= 0) {
         return sendError(res, 'Invalid commission parameters.', 400);
    }

    const result = await processCommission(referrerId, refereeId, sourceReward);

    if (result.ok) {
        sendSuccess(res, { message: 'Commission processed.', commission_amount: result.commission });
    } else {
        sendError(res, result.error, 400);
    }
}


// ------------------------------------------------------------------
// Main Handler
// ------------------------------------------------------------------

module.exports = async (req, res) => {
  if (req.method !== 'POST') {
    return sendError(res, 'Method Not Allowed', 405);
  }

  let body;
  try {
    // Await the body parsing
    const data = await new Promise((resolve, reject) => {
        let data = '';
        req.on('data', (chunk) => {
            data += chunk.toString();
        });
        req.on('end', () => {
            try {
                resolve(JSON.parse(data));
            } catch (e) {
                reject(new Error('Invalid JSON payload.'));
            }
        });
        req.on('error', reject);
    });
    body = data;
  } catch (error) {
    return sendError(res, error.message, 400);
  }

  if (!body || !body.type) {
    return sendError(res, 'Missing "type" field in the request body.', 400);
  }

  // ⬅️ initData Security Check
  if (body.type !== 'commission' && body.type !== 'generateActionId' && (!body.initData || !validateInitData(body.initData))) {
      return sendError(res, 'Invalid or expired initData. Security check failed.', 401);
  }

  if (!body.user_id && body.type !== 'commission') {
      return sendError(res, 'Missing user_id in the request body.', 400);
  }

  // Route the request based on the 'type' field
  switch (body.type) {
    case 'generateActionId': // ⬅️ Must be first (no user_id needed)
        await handleGenerateActionId(req, res, body);
        break;
    case 'getUserData':
      await handleGetUserData(req, res, body);
      break;
    case 'register':
      await handleRegister(req, res, body);
      break;
    case 'watchAd':
      await handleWatchAd(req, res, body);
      break;
    case 'commission':
      await handleCommission(req, res, body);
      break;
    case 'preSpin': 
      await handlePreSpin(req, res, body);
      break;
    case 'spinResult': 
      await handleSpinResult(req, res, body);
      break;
    case 'withdraw':
      await handleWithdraw(req, res, body);
      break;
    case 'completeTask': // ⬅️ Original Task System
      await handleCompleteTask(req, res, body);
      break;
      
    // ⬇️ NEW CODE ADDITION START: NEW Task System Routes
    case 'getNewTasks': 
      await handleGetNewTasks(req, res, body);
      break;
    case 'completeNewTask': 
      await handleCompleteNewTask(req, res, body);
      break;
    // ⬆️ NEW CODE ADDITION END

    default:
      sendError(res, `Unknown request type: ${body.type}`, 400);
      break;
  }
}