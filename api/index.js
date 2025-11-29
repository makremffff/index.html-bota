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
        // تحديث last_activity سيتم لاحقاً في دوال watchAd/spinResult
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
        await supabaseFetch('commission_history', 'POST', { referrer_id: referrerId, referee_id: refereeId, amount: commissionAmount, source_reward: sourceReward }, '?select=referrer_id');

        return { ok: true, new_referrer_balance: newBalance };
    } catch (error) {
        console.error(`Commission failed for referrer ${referrerId}:`, error.message);
        return { ok: false, error: error.message };
    }
}

// ------------------------------------------------------------------
// 🔐 Action ID Security Functions (Added new task type to handleGenerateActionId)
// ------------------------------------------------------------------
/**
 * Generates a unique, time-limited Action ID (token) for a specific action and user.
 * This token must be consumed immediately by the next request.
 */
async function handleGenerateActionId(req, res, body) {
    const { user_id, action_type } = body;
    const id = parseInt(user_id);

    // ⚠️ New check for the new task type
    const validActionTypes = ['watchAd', 'preSpin', 'spinResult', 'withdraw', 'completeTask', 'completeNewTask'];
    if (!validActionTypes.includes(action_type)) {
         return sendError(res, `Invalid action_type: ${action_type}`, 400);
    }

    try {
        // Simple security check for existing user/ban status before token issue
        const users = await supabaseFetch('users', 'GET', null, `?id=eq.${id}&select=is_banned`);
        if (!Array.isArray(users) || users.length === 0 || users[0].is_banned) {
            return sendError(res, 'User not found or banned.', 403);
        }

        // Generate a cryptographically secure random token (e.g., UUID or short hash)
        const actionId = crypto.randomBytes(16).toString('hex');
        
        // Save the token with expiry time (created_at) and action type
        await supabaseFetch('temp_actions', 'POST', 
            { user_id: id, action_id: actionId, action_type: action_type }, 
            '?select=id');

        sendSuccess(res, { action_id: actionId });
    } catch (error) {
        console.error('Failed to generate and save action ID:', error.message);
        sendError(res, 'Failed to generate security token.', 500);
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
             await supabaseFetch('temp_actions', 'DELETE', null, `?id=eq.${record.id}`);
             sendError(res, 'Server Token (Action ID) expired. Please retry.', 408);
             return false;
        }

        // 2. Consume/Delete the token to prevent replay attack
        await supabaseFetch('temp_actions', 'DELETE', null, `?id=eq.${record.id}`);
        
        return true;
    } catch (error) {
        console.error('Action ID validation failed:', error.message);
        sendError(res, 'Security token validation failed.', 500);
        return false;
    }
}


// ------------------------------------------------------------------
// 1) type: "getUserData" (No change)
// ------------------------------------------------------------------
async function handleGetUserData(req, res, body) {
    const { user_id } = body;
    const id = parseInt(user_id);
    
    try {
        // 0. Perform Limit Reset Check/Update BEFORE fetching data
        await resetDailyLimitsIfExpired(id);

        // 1. Fetch user data
        const users = await supabaseFetch('users', 'GET', null, `?id=eq.${id}&select=id,balance,is_banned,ads_watched_today,spins_today,ref_by,task_completed`);
        if (!Array.isArray(users) || users.length === 0) {
            return sendError(res, 'User not found.', 404);
        }
        const userData = users[0];
        
        // 2. Count referrals
        const referrals = await supabaseFetch('users', 'GET', null, `?ref_by=eq.${id}&select=id`);
        const referralsCount = referrals.length;

        // 3. Fetch withdrawal history (top 10)
        const withdrawalHistory = await supabaseFetch('withdrawal_history', 'GET', null, `?user_id=eq.${id}&order=created_at.desc&limit=10`);

        // 4. Update last_activity (for rate limiting, even on read requests now)
        await supabaseFetch('users', 'PATCH', { last_activity: new Date().toISOString() }, `?id=eq.${id}&select=id`);

        sendSuccess(res, { ...userData, referrals_count: referralsCount, withdrawal_history: withdrawalHistory });

    } catch (error) {
        console.error('GetUserData failed:', error.message);
        sendError(res, `Failed to retrieve user data: ${error.message}`, 500);
    }
}

/**
 * 2) type: "register"
 * ⚠️ Fix: Includes task_completed: false for new users.
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
                ref_by: ref_by ? parseInt(ref_by) : null,
                last_activity: new Date().toISOString(), // ⬅️ يبقى هنا للتسجيل الأولي
                task_completed: false // ⬅️ NEW DEFAULT
            };

            await supabaseFetch('users', 'POST', newUser, '?select=id');
            console.log(`New user registered: ${id}`);
        } else {
             // 3. User exists, check for ban status
             if (users[0].is_banned) {
                 return sendError(res, 'User is banned.', 403);
             }
        }
        
        // 4. Success: User is either new or existing and active
        sendSuccess(res, { message: 'User registered/verified successfully.' });
    } catch (error) {
        console.error('Registration failed:', error.message);
        sendError(res, `Registration failed: ${error.message}`, 500);
    }
}

/**
 * 3) type: "watchAd" (No change in core logic)
 */
async function handleWatchAd(req, res, body) {
    const { user_id, action_id } = body;
    const id = parseInt(user_id);
    
    // 1. Validate Action ID (Security)
    if (!await validateAndUseActionId(res, id, action_id, 'watchAd')) {
        return;
    }

    try {
        // 2. Fetch User Data (required for limit check, balance, and referrer)
        const users = await supabaseFetch('users', 'GET', null, `?id=eq.${id}&select=id,balance,is_banned,ads_watched_today,ref_by`);
        
        if (!Array.isArray(users) || users.length === 0) {
            return sendError(res, 'User not found.', 404);
        }
        const user = users[0];
        
        // 3. Banned Check
        if (user.is_banned) {
            return sendError(res, 'User is banned.', 403);
        }

        // 4. Rate Limit Check (already handled by validateAndUseActionId, but for robustness)
        const rateLimitResult = await checkRateLimit(id);
        if (!rateLimitResult.ok) {
            return sendError(res, rateLimitResult.message, 429);
        }

        // 5. Check maximum ad limit
        if (user.ads_watched_today >= DAILY_MAX_ADS) {
            return sendError(res, `Daily ad limit (${DAILY_MAX_ADS}) reached.`, 403);
        }

        // --- All checks passed: Process Ad Reward ---
        const reward = REWARD_PER_AD;
        const newBalance = user.balance + reward;
        const newAdsCount = user.ads_watched_today + 1;
        const referrerId = user.ref_by;
        
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
 * 4) type: "commission" (No change)
 */
async function handleCommission(req, res, body) {
    const { referrer_id, referee_id, source_reward } = body;
    const referrerId = parseInt(referrer_id);
    const refereeId = parseInt(referee_id);
    const reward = parseFloat(source_reward);

    const result = await processCommission(referrerId, refereeId, reward);
    
    if (result.ok) {
        sendSuccess(res, { new_referrer_balance: result.new_referrer_balance });
    } else {
        sendError(res, result.error, 500);
    }
}

/**
 * 5) type: "preSpin" (No change)
 */
async function handlePreSpin(req, res, body) {
    const { user_id, action_id } = body;
    const id = parseInt(user_id);
    
    // 1. Validate Action ID (Security)
    if (!await validateAndUseActionId(res, id, action_id, 'preSpin')) {
        return;
    }

    try {
        // 2. Fetch User Data
        const users = await supabaseFetch('users', 'GET', null, `?id=eq.${id}&select=is_banned,spins_today`);
        
        if (!Array.isArray(users) || users.length === 0) {
            return sendError(res, 'User not found.', 404);
        }
        const user = users[0];
        
        // 3. Banned Check
        if (user.is_banned) {
            return sendError(res, 'User is banned.', 403);
        }

        // 4. Check maximum spin limit
        if (user.spins_today >= DAILY_MAX_SPINS) {
            return sendError(res, `Daily spin limit (${DAILY_MAX_SPINS}) reached.`, 403);
        }
        
        // 5. Success
        sendSuccess(res, { message: 'Pre-spin checks passed.' });

    } catch (error) {
        console.error('PreSpin failed:', error.message);
        sendError(res, `Failed to process pre-spin: ${error.message}`, 500);
    }
}

/**
 * 6) type: "spinResult" (No change in core logic)
 */
async function handleSpinResult(req, res, body) {
    const { user_id, action_id } = body;
    const id = parseInt(user_id);
    
    // 1. Validate Action ID (Security)
    if (!await validateAndUseActionId(res, id, action_id, 'spinResult')) {
        return;
    }

    try {
        // 2. Fetch User Data (required for limit check, balance, and referrer)
        const users = await supabaseFetch('users', 'GET', null, `?id=eq.${id}&select=id,balance,is_banned,spins_today,ref_by`);
        
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
        const referrerId = user.ref_by;

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
 * 7) type: "completeTask" (Original Channel Join Task - No change)
 */
async function handleCompleteTask(req, res, body) {
    const { user_id, action_id } = body;
    const id = parseInt(user_id);
    const reward = TASK_REWARD;

    // 1. Validate Action ID (Security)
    if (!await validateAndUseActionId(res, id, action_id, 'completeTask')) {
        return;
    }

    try {
        // 2. Fetch User Data and check status
        const users = await supabaseFetch('users', 'GET', null, `?id=eq.${id}&select=id,balance,is_banned,ref_by,task_completed`);
        if (!Array.isArray(users) || users.length === 0) {
            return sendError(res, 'User not found.', 404);
        }
        const user = users[0];
        const referrerId = user.ref_by;
        
        // 3. Banned/Completed Check
        if (user.is_banned) {
            return sendError(res, 'User is banned.', 403);
        }
        if (user.task_completed) {
            return sendError(res, 'Task already completed.', 403);
        }

        // 4. Rate Limit Check
        const rateLimitResult = await checkRateLimit(id);
        if (!rateLimitResult.ok) {
            return sendError(res, rateLimitResult.message, 429);
        }

        // 5. Telegram Channel Membership Check
        const isMember = await checkChannelMembership(id, TELEGRAM_CHANNEL_USERNAME);

        if (!isMember) {
            return sendError(res, 'User has not joined the required Telegram channel.', 400);
        }

        // 6. Process Reward and Update User Data
        const newBalance = user.balance + reward;
        const updatePayload = {
            balance: newBalance,
            task_completed: true, // Mark as completed
            last_activity: new Date().toISOString() // Update for Rate Limit
        };

        await supabaseFetch('users', 'PATCH', updatePayload, `?id=eq.${id}`);
        
        // 7. Commission Call
        if (referrerId) {
            processCommission(referrerId, id, reward).catch(e => {
                console.error(`CompleteTask Commission failed silently for referrer ${referrerId}:`, e.message);
            });
        }

        // 8. Success
        sendSuccess(res, { new_balance: newBalance, actual_reward: reward, message: 'Task completed successfully.' });

    } catch (error) {
        console.error('CompleteTask failed:', error.message);
        sendError(res, `Failed to complete task: ${error.message}`, 500);
    }
}

// ------------------------------------------------------------------
// 8) type: "getNewTasks" (NEW API)
// ------------------------------------------------------------------
/**
 * 8) type: "getNewTasks"
 * Returns all active and available tasks from tasks_new.
 */
async function handleGetNewTasks(req, res, body) {
    const { user_id } = body;
    const id = parseInt(user_id);
    
    try {
        // 1. Check if user is banned
        const users = await supabaseFetch('users', 'GET', null, `?id=eq.${id}&select=is_banned`);
        if (!Array.isArray(users) || users.length === 0) {
            return sendError(res, 'User not found.', 404);
        }
        if (users[0].is_banned) {
            return sendError(res, 'User is banned.', 403);
        }
        
        // 2. Fetch tasks that are active and not full
        // Note: Using a single query to filter is not possible with Supabase REST API limitations (e.g., current_users < max_users)
        // We will fetch active tasks and filter them later for max_users limit if necessary, but the API can do basic filtering.
        const availableTasks = await supabaseFetch('tasks_new', 'GET', null, 
            `?select=id,task_name,task_url,task_reward,max_users,current_users&is_active=eq.true`);
        
        // 3. Fetch tasks completed by the current user
        const completedTasks = await supabaseFetch('user_tasks_new', 'GET', null, `?user_id=eq.${id}&select=task_id`);
        const completedTaskIds = new Set(completedTasks.map(t => t.task_id));
        
        // 4. Filter for available tasks and merge data
        const tasksWithStatus = availableTasks
            .filter(task => task.current_users < task.max_users) // Manual check for max_users
            .map(task => ({
                id: task.id,
                task_name: task.task_name,
                task_url: task.task_url,
                task_reward: parseFloat(task.task_reward), // Ensure it's a number
                max_users: task.max_users,
                current_users: task.current_users,
                is_completed: completedTaskIds.has(task.id)
            }));

        sendSuccess(res, { tasks: tasksWithStatus });
    } catch (error) {
        console.error('GetNewTasks failed:', error.message);
        sendError(res, `Failed to retrieve new tasks: ${error.message}`, 500);
    }
}

// ------------------------------------------------------------------
// 9) type: "completeNewTask" (NEW API)
// ------------------------------------------------------------------
/**
 * 9) type: "completeNewTask"
 * Handles the completion of a specific task from tasks_new.
 */
async function handleCompleteNewTask(req, res, body) {
    const { user_id, task_id, action_id } = body;
    const id = parseInt(user_id);
    const taskId = parseInt(task_id);
    
    // 1. Validate Action ID (Security)
    if (!await validateAndUseActionId(res, id, action_id, 'completeNewTask')) {
        return;
    }

    try {
        // 2. Fetch User and Task Data in a transaction-like manner
        
        // a. Fetch User (needed for balance and referrer)
        const users = await supabaseFetch('users', 'GET', null, `?id=eq.${id}&select=id,balance,is_banned,ref_by`);
        if (!Array.isArray(users) || users.length === 0) {
            return sendError(res, 'User not found.', 404);
        }
        const user = users[0];
        if (user.is_banned) {
            return sendError(res, 'User is banned.', 403);
        }
        
        // b. Check if already completed (user_tasks_new)
        const completed = await supabaseFetch('user_tasks_new', 'GET', null, `?user_id=eq.${id}&task_id=eq.${taskId}&select=id`);
        if (completed.length > 0) {
            return sendError(res, 'Task already completed by user.', 403);
        }

        // c. Fetch Task details (tasks_new)
        const tasks = await supabaseFetch('tasks_new', 'GET', null, `?id=eq.${taskId}&select=task_reward,max_users,current_users,is_active`);
        if (!Array.isArray(tasks) || tasks.length === 0) {
            return sendError(res, 'Task not found or inactive.', 404);
        }
        const task = tasks[0];
        
        // 3. Check Task Limits
        if (!task.is_active) {
            return sendError(res, 'Task is inactive.', 403);
        }
        if (task.current_users >= task.max_users) {
            return sendError(res, 'Task has reached maximum user capacity.', 403);
        }
        
        // 4. Check Rate Limit
        const rateLimitResult = await checkRateLimit(id);
        if (!rateLimitResult.ok) {
            return sendError(res, rateLimitResult.message, 429);
        }
        
        const reward = parseFloat(task.task_reward);
        
        // 5. Update Task Counter (tasks_new)
        const newCurrentUsers = task.current_users + 1;
        await supabaseFetch('tasks_new', 'PATCH', 
            { current_users: newCurrentUsers }, 
            `?id=eq.${taskId}&select=id`);
            
        // 6. Record Completion (user_tasks_new)
        await supabaseFetch('user_tasks_new', 'POST', 
            { user_id: id, task_id: taskId }, 
            '?select=id');

        // 7. Update User Balance and Activity (original 'users' table)
        const newBalance = user.balance + reward;
        const updatePayload = { 
            balance: newBalance,
            last_activity: new Date().toISOString() // Update for Rate Limit
        };
        
        await supabaseFetch('users', 'PATCH', 
            updatePayload, 
            `?id=eq.${id}&select=id`);

        // 8. Process Commission
        if (user.ref_by) {
            processCommission(user.ref_by, id, reward).catch(e => {
                console.error(`CompleteNewTask Commission failed silently for referrer ${user.ref_by}:`, e.message);
            });
        }

        // 9. Success
        sendSuccess(res, { 
            new_balance: newBalance, 
            actual_reward: reward, 
            new_current_users: newCurrentUsers,
            message: 'Task completed and reward claimed successfully.' 
        });

    } catch (error) {
        console.error('CompleteNewTask failed:', error.message);
        sendError(res, `Failed to complete new task: ${error.message}`, 500);
    }
}

/**
 * 10) type: "withdraw" (No change)
 */
async function handleWithdraw(req, res, body) {
    const { user_id, binanceId, amount, action_id } = body;
    const id = parseInt(user_id);
    const withdrawAmount = parseFloat(amount);
    
    // 1. Validate Action ID (Security)
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

        // 5. Check Minimum Withdrawal and Balance
        if (withdrawAmount <= 0) {
            return sendError(res, 'Withdrawal amount must be greater than zero.', 400);
        }
        // Assuming a minimum withdrawal of 1000 SHIB based on frontend logic inferred
        if (withdrawAmount < 1000) { 
             return sendError(res, 'Minimum withdrawal amount is 1,000 SHIB.', 400);
        }
        if (user.balance < withdrawAmount) {
            return sendError(res, 'Insufficient balance.', 400);
        }
        
        // 6. Process Withdrawal
        const newBalance = user.balance - withdrawAmount;
        
        // 7. Update User Balance and Activity
        const updatePayload = {
            balance: newBalance,
            last_activity: new Date().toISOString() // Update for Rate Limit
        };
        await supabaseFetch('users', 'PATCH', updatePayload, `?id=eq.${id}`);

        // 8. Log Withdrawal Request
        await supabaseFetch('withdrawal_history', 'POST', 
            { user_id: id, binance_id: binanceId, amount: withdrawAmount, status: 'pending' }, 
            '?select=id');

        // 9. Success
        sendSuccess(res, { new_balance: newBalance, message: 'Withdrawal request submitted successfully.' });

    } catch (error) {
        console.error('Withdraw failed:', error.message);
        sendError(res, `Failed to process withdrawal: ${error.message}`, 500);
    }
}


// ------------------------------------------------------------------
// Main Handler (module.exports)
// ------------------------------------------------------------------
module.exports = async (req, res) => {
  if (req.method !== 'POST') {
    return sendError(res, 'Only POST requests are allowed.', 405);
  }

  let body;
  try {
    body = await new Promise((resolve, reject) => {
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

  } catch (error) {
    return sendError(res, error.message, 400);
  }

  if (!body || !body.type) {
    return sendError(res, 'Missing "type" field in the request body.', 400);
  }

  // ⬅️ initData Security Check
  if (body.type !== 'commission' && (!body.initData || !validateInitData(body.initData))) {
      return sendError(res, 'Invalid or expired initData. Security check failed.', 401);
  }

  if (!body.user_id && body.type !== 'commission') {
      return sendError(res, 'Missing user_id in the request body.', 400);
  }

  // Route the request based on the 'type' field
  switch (body.type) {
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
    case 'completeTask': // ⬅️ Original Task
      await handleCompleteTask(req, res, body);
      break;
    case 'generateActionId': 
        await handleGenerateActionId(req, res, body);
        break;
    // ⬇️ NEW API ENDPOINTS
    case 'getNewTasks': 
        await handleGetNewTasks(req, res, body);
        break;
    case 'completeNewTask': 
        await handleCompleteNewTask(req, res, body);
        break;
    // ⬆️ END NEW API ENDPOINTS
    default:
      sendError(res, `Unknown request type: ${body.type}`, 400);
  }
};
