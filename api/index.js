// /api/index.js (Final and Secure Version with Dynamic Tasks)

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
// OLD Task Constants - REMOVED/Commented Out for Dynamic Tasks
// ------------------------------------------------------------------
// const TASK_REWARD = 50; // الآن يتم جلبها من جدول dynamic_tasks
// const TELEGRAM_CHANNEL_USERNAME = '@botbababab'; // الآن يمكن أن يكون الرابط في جدول dynamic_tasks


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
          // Check for DELETE/UPDATE success without content
          if (method === 'DELETE' || method === 'PATCH') {
               // PATCH/DELETE success returns an array of the updated/deleted rows, or an empty array.
              return Array.isArray(jsonResponse) ? jsonResponse : [{ success: true }];
          }
          return Array.isArray(jsonResponse) ? jsonResponse : jsonResponse;
      } catch (e) {
          // If response is OK but not JSON (e.g., POST with 'return=minimal')
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
                // ⚠️ 6 hours passed since the limit was reached, reset counter
                updatePayload.ads_watched_today = 0;
                updatePayload.ads_limit_reached_at = null; // remove lock time
                console.log(`Ads limit reset for user ${userId}.`);
            }
        }

        // 3. Check Spins Limit Reset
        if (user.spins_limit_reached_at && user.spins_today >= DAILY_MAX_SPINS) {
            const spinsLimitTime = new Date(user.spins_limit_reached_at).getTime();
            if (now - spinsLimitTime > RESET_INTERVAL_MS) {
                // ⚠️ 6 hours passed since the limit was reached, reset counter
                updatePayload.spins_today = 0;
                updatePayload.spins_limit_reached_at = null; // remove lock time
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
 */
async function checkRateLimit(userId) {
    try {
        const users = await supabaseFetch('users', 'GET', null, `?id=eq.${userId}&select=last_activity`);
        if (!ArrayOfUsers(users)) { // Checking if the returned value is a non-empty array
            return { ok: true };
        }

        const user = users[0];
        // If last_activity is null, assume 0 to allow the request
        const lastActivity = user.last_activity ? new Date(user.last_activity).getTime() : 0; 
        const now = Date.now();
        const timeElapsed = now - lastActivity;

        if (timeElapsed < MIN_TIME_BETWEEN_ACTIONS_MS) {
            return { 
                ok: false, 
                message: `Please wait ${((MIN_TIME_BETWEEN_ACTIONS_MS - timeElapsed) / 1000).toFixed(1)} seconds before another action. (Rate Limit)` 
            };
        }
        
        // ⚠️ Rate limit passed, update last_activity for the next check
        // This MUST be done before the main action to prevent race conditions during high load
        await supabaseFetch('users', 'PATCH', 
            { last_activity: new Date().toISOString() }, 
            `?id=eq.${userId}`
        );
        
        return { ok: true };
    } catch (error) {
        console.error(`Failed to check/update rate limit for user ${userId}:`, error.message);
        return { ok: false, message: 'Server error during rate limit check.' };
    }
}

/**
 * Validate Telegram's initData hash.
 */
function validateInitData(initData) {
    if (!BOT_TOKEN) {
        console.error('Security Warning: BOT_TOKEN is not configured. initData validation skipped.');
        return true; 
    }

    try {
        const params = new URLSearchParams(initData);
        const hash = params.get('hash');
        if (!hash) return false;

        const dataCheckArr = [];
        // Extract all keys except 'hash' and sort them alphabetically
        for (const [key, value] of params.entries()) {
            if (key !== 'hash') {
                dataCheckArr.push(`${key}=${value}`);
            }
        }
        dataCheckArr.sort();
        const dataCheckString = dataCheckArr.join('\n');

        // Create the secret key: HMAC_SHA256(bot_token, "WebAppData")
        const secretKey = crypto.createHmac('sha256', 'WebAppData')
                              .update(BOT_TOKEN)
                              .digest();

        // Calculate the hash: HMAC_SHA256(secret_key, data_check_string)
        const calculatedHash = crypto.createHmac('sha256', secretKey)
                                     .update(dataCheckString)
                                     .digest('hex');

        return calculatedHash === hash;
    } catch (e) {
        console.error('InitData validation error:', e.message);
        return false;
    }
}

function ArrayOfUsers(users) {
    return Array.isArray(users) && users.length > 0;
}

// ------------------------------------------------------------------
// API Handlers (Request Router)
// ------------------------------------------------------------------

/**
 * 1) type: "register"
 * Handles user registration and referrer linkage.
 */
async function handleRegister(req, res, body) {
    const { user_id, username, first_name, referral_id } = body;
    const id = parseInt(user_id);
    const refId = referral_id ? parseInt(referral_id) : null;

    try {
        // 1. Check if user already exists
        const existingUsers = await supabaseFetch('users', 'GET', null, `?id=eq.${id}&select=id`);

        if (ArrayOfUsers(existingUsers)) {
            // User already exists, simply return success
            return sendSuccess(res, { message: 'User already registered.' });
        }

        let referrerExists = false;
        if (refId && refId !== id) {
             // 2. Check if the referrer ID exists
            const referrerCheck = await supabaseFetch('users', 'GET', null, `?id=eq.${refId}&select=id`);
            if (ArrayOfUsers(referrerCheck)) {
                referrerExists = true;
            } else {
                console.log(`Referrer ID ${refId} not found.`);
            }
        }

        // 3. Create new user record
        const newUser = {
            id: id,
            username: username,
            first_name: first_name,
            balance: 0.0,
            referral_id: referrerExists ? refId : null,
            ads_watched_today: 0,
            spins_today: 0,
            is_banned: false,
            // task_completed: false, // يتم إدارة المهام الآن عبر جدول user_tasks
            last_activity: new Date().toISOString()
        };

        const newUserData = await supabaseFetch('users', 'POST', newUser);
        
        sendSuccess(res, { message: 'User registered successfully.', user: newUserData });

    } catch (error) {
        console.error('Registration failed:', error.message);
        sendError(res, `Registration failed: ${error.message}`, 500);
    }
}


/**
 * 2) type: "getUserData"
 * Fetches user data, resets limits if needed, and calculates referral count.
 */
async function handleGetUserData(req, res, body) {
    const { user_id } = body;
    const id = parseInt(user_id);

    try {
        // 1. Reset limits if required
        await resetDailyLimitsIfExpired(id);

        // 2. Fetch user data
        const users = await supabaseFetch('users', 'GET', null, `?id=eq.${id}&select=*,withdrawals(id,status,amount,created_at)`);

        if (!ArrayOfUsers(users)) {
            return sendError(res, 'User not found. Please register first.', 404);
        }

        const user = users[0];
        if (user.is_banned) {
            return sendError(res, 'User is banned.', 403);
        }

        // 3. Fetch referral count
        const referrals = await supabaseFetch('users', 'GET', null, `?referral_id=eq.${id}&select=id`);
        const referral_count = ArrayOfUsers(referrals) ? referrals.length : 0;

        // 4. Combine data and return
        const userData = {
            ...user,
            referral_count: referral_count,
            // The frontend now fetches task data using handleGetTaskData
        };

        sendSuccess(res, { user: userData });

    } catch (error) {
        console.error('GetUserData failed:', error.message);
        sendError(res, `Failed to retrieve user data: ${error.message}`, 500);
    }
}

/**
 * 3) type: "watchAd"
 * Handles ad watching and balance/counter updates.
 */
async function handleWatchAd(req, res, body) {
    const { user_id } = body;
    const id = parseInt(user_id);
    const reward = REWARD_PER_AD;

    try {
        // 1. Rate Limit Check (updates last_activity)
        const rateLimitResult = await checkRateLimit(id);
        if (!rateLimitResult.ok) {
            return sendError(res, rateLimitResult.message, 429);
        }

        // 2. Fetch user data (after rate limit check to ensure last_activity is updated)
        const users = await supabaseFetch('users', 'GET', null, `?id=eq.${id}&select=id,balance,referral_id,ads_watched_today,ads_limit_reached_at,is_banned`);
        
        if (!ArrayOfUsers(users)) {
            return sendError(res, 'User not found.', 404);
        }
        const user = users[0];
        if (user.is_banned) {
            return sendError(res, 'User is banned.', 403);
        }

        // 3. Check daily limit
        if (user.ads_limit_reached_at && user.ads_watched_today >= DAILY_MAX_ADS) {
            return sendError(res, 'Daily ad viewing limit reached. Try again in 6 hours.', 403);
        }

        // 4. Update user data
        let newAdsWatched = user.ads_watched_today + 1;
        let newBalance = user.balance + reward;
        let limitReachedAt = user.ads_limit_reached_at;

        if (newAdsWatched >= DAILY_MAX_ADS) {
            limitReachedAt = limitReachedAt || new Date().toISOString();
        }

        const updatePayload = {
            balance: newBalance,
            ads_watched_today: newAdsWatched,
            ads_limit_reached_at: limitReachedAt,
            // last_activity already updated by checkRateLimit
        };
        await supabaseFetch('users', 'PATCH', updatePayload, `?id=eq.${id}`);


        // 5. Handle Referral Commission (separate transaction for reliability)
        let commission = 0;
        if (user.referral_id) {
            commission = reward * REFERRAL_COMMISSION_RATE;
            await supabaseFetch('users', 'RPC', { 
                function_name: 'update_referrer_balance', 
                _ref_id: user.referral_id, 
                _commission: commission 
            });
        }


        // 6. Success response
        sendSuccess(res, { 
            new_balance: newBalance, 
            reward: reward, 
            commission: commission,
            ads_watched_today: newAdsWatched
        });

    } catch (error) {
        console.error('WatchAd failed:', error.message);
        sendError(res, `Failed to watch ad: ${error.message}`, 500);
    }
}


/**
 * 4) type: "commission"
 * Fetches the total commission earned. (Simple GET request)
 */
async function handleCommission(req, res, body) {
    const { user_id } = body;
    const id = parseInt(user_id);

    try {
        // Fetch commission records for simplicity (or calculate from a dedicated column if available)
        // Since we don't have a dedicated commission column/table, we'll return 0 or rely on a more complex DB function.
        // Given the RPC update_referrer_balance is used, we assume commission is integrated into the referrer's balance.
        // For now, we only return the commission rate and instructions for the client.
        
        sendSuccess(res, { 
            referral_rate: REFERRAL_COMMISSION_RATE * 100, 
            message: 'Commission is automatically added to the referrer\'s balance upon ad completion by the referral.'
        });

    } catch (error) {
        console.error('Commission failed:', error.message);
        sendError(res, `Failed to retrieve commission data: ${error.message}`, 500);
    }
}


/**
 * 5) type: "preSpin"
 * Handles the logic before the spin, checks limits.
 */
async function handlePreSpin(req, res, body) {
    const { user_id } = body;
    const id = parseInt(user_id);
    
    try {
        // 1. Rate Limit Check
        const rateLimitResult = await checkRateLimit(id);
        if (!rateLimitResult.ok) {
            return sendError(res, rateLimitResult.message, 429);
        }

        // 2. Fetch user data
        const users = await supabaseFetch('users', 'GET', null, `?id=eq.${id}&select=spins_today,spins_limit_reached_at,is_banned`);
        
        if (!ArrayOfUsers(users)) {
            return sendError(res, 'User not found.', 404);
        }
        const user = users[0];
        if (user.is_banned) {
            return sendError(res, 'User is banned.', 403);
        }

        // 3. Check daily limit
        if (user.spins_limit_reached_at && user.spins_today >= DAILY_MAX_SPINS) {
            return sendError(res, 'Daily spin limit reached. Try again in 6 hours.', 403);
        }
        
        // 4. Success - allows the client to proceed with spin animation
        sendSuccess(res, { message: 'Ready to spin.' });

    } catch (error) {
        console.error('PreSpin failed:', error.message);
        sendError(res, `Failed to prepare spin: ${error.message}`, 500);
    }
}

/**
 * 6) type: "spinResult"
 * Handles the final spin result, prize calculation, and balance update.
 */
async function handleSpinResult(req, res, body) {
    const { user_id } = body;
    const id = parseInt(user_id);
    
    try {
        // 1. Fetch user data
        const users = await supabaseFetch('users', 'GET', null, `?id=eq.${id}&select=id,balance,spins_today,spins_limit_reached_at,is_banned`);
        
        if (!ArrayOfUsers(users)) {
            return sendError(res, 'User not found.', 404);
        }
        const user = users[0];
        if (user.is_banned) {
            return sendError(res, 'User is banned.', 403);
        }

        // 2. Re-check daily limit (critical for security)
        if (user.spins_limit_reached_at && user.spins_today >= DAILY_MAX_SPINS) {
            return sendError(res, 'Daily spin limit reached. Try again in 6 hours.', 403);
        }

        // 3. Calculate Prize
        const { prize, prizeIndex } = calculateRandomSpinPrize();

        // 4. Update user data
        let newSpinsToday = user.spins_today + 1;
        let newBalance = user.balance + prize;
        let limitReachedAt = user.spins_limit_reached_at;

        if (newSpinsToday >= DAILY_MAX_SPINS) {
            limitReachedAt = limitReachedAt || new Date().toISOString();
        }

        const updatePayload = {
            balance: newBalance,
            spins_today: newSpinsToday,
            spins_limit_reached_at: limitReachedAt,
            // last_activity was updated in preSpin/checkRateLimit
        };
        await supabaseFetch('users', 'PATCH', updatePayload, `?id=eq.${id}`);

        // 5. Success response
        sendSuccess(res, { 
            new_balance: newBalance, 
            prize: prize, 
            prize_index: prizeIndex, // Send index to client for animation sync
            spins_today: newSpinsToday
        });

    } catch (error) {
        console.error('SpinResult failed:', error.message);
        sendError(res, `Failed to process spin: ${error.message}`, 500);
    }
}


/**
 * 7) type: "withdraw"
 * Handles the withdrawal request and logs it to the 'withdrawals' table.
 */
async function handleWithdraw(req, res, body) {
    const { user_id, binanceId, amount } = body;
    const id = parseInt(user_id);
    const withdrawalAmount = parseFloat(amount);
    
    // Simple validation
    if (!binanceId || withdrawalAmount <= 0 || isNaN(withdrawalAmount)) {
        return sendError(res, 'Invalid withdrawal details (Binance ID or Amount).', 400);
    }
    
    try {
        // 1. Fetch user data (Lock for balance update)
        const users = await supabaseFetch('users', 'GET', null, `?id=eq.${id}&select=id,balance,is_banned`);
        
        if (!ArrayOfUsers(users)) {
            return sendError(res, 'User not found.', 404);
        }
        const user = users[0];
        if (user.is_banned) {
            return sendError(res, 'User is banned.', 403);
        }

        // 2. Check balance sufficiency
        if (user.balance < withdrawalAmount) {
            return sendError(res, 'Insufficient balance for this withdrawal amount.', 403);
        }

        // 3. Update user balance (deduct amount)
        const newBalance = user.balance - withdrawalAmount;
        await supabaseFetch('users', 'PATCH', { balance: newBalance }, `?id=eq.${id}`);

        // 4. Log withdrawal request
        const withdrawalRecord = {
            user_id: id,
            binance_id: binanceId,
            amount: withdrawalAmount,
            status: 'Pending' // Initial status
        };
        await supabaseFetch('withdrawals', 'POST', withdrawalRecord);

        // 5. Success response
        sendSuccess(res, { 
            new_balance: newBalance, 
            message: 'Withdrawal request submitted successfully.' 
        });

    } catch (error) {
        console.error('Withdrawal failed:', error.message);
        // Note: In a real system, you should roll back the balance deduction if the withdrawal logging fails.
        sendError(res, `Failed to submit withdrawal: ${error.message}`, 500);
    }
}

/**
 * 8) type: "getTaskData"
 * Handles fetching the currently active task for the user.
 * Fetches the first active task from dynamic_tasks where completed_users < max_users,
 * AND the user hasn't completed it yet.
 */
async function handleGetTaskData(req, res, body) {
    const { user_id } = body;
    const id = parseInt(user_id);

    try {
        // 1. Fetch user to ensure existence
        const users = await supabaseFetch('users', 'GET', null, `?id=eq.${id}&select=id,is_banned`);
        if (!ArrayOfUsers(users)) {
            return sendError(res, 'User not found.', 404);
        }
        if (users[0].is_banned) {
            return sendError(res, 'User is banned.', 403);
        }

        // 2. Fetch tasks the user has already completed
        const completedTasks = await supabaseFetch('user_tasks', 'GET', null, `?user_id=eq.${id}&select=task_id`);
        const completedTaskIds = completedTasks.map(t => t.task_id);

        // 3. Build the query to find an active, un-completed task
        let query = `?is_active=eq.true&completed_users=lt.max_users&select=id,name,reward,link,max_users,completed_users`;
        
        // Add exclusion for completed tasks (Supabase REST API array filter)
        if (completedTaskIds.length > 0) {
            const exclusionList = completedTaskIds.join(',');
            query += `&id=not.in.(${exclusionList})`;
        }
        
        // Limit to one task and order by ID (e.g., oldest first)
        query += `&limit=1&order=id.asc`; 

        const tasks = await supabaseFetch('dynamic_tasks', 'GET', null, query);

        if (!ArrayOfUsers(tasks)) {
            return sendSuccess(res, { task: null, message: 'No available tasks found.' });
        }

        // Return the first available task (includes name, reward, link, max_users)
        sendSuccess(res, { task: tasks[0] });

    } catch (error) {
        console.error('handleGetTaskData failed:', error.message);
        sendError(res, `Failed to retrieve task data: ${error.message}`, 500);
    }
}

/**
 * 9) type: "completeTask"
 * Handles dynamic task completion and reward based on task_id.
 */
async function handleCompleteTask(req, res, body) {
    // Requires: user_id, task_id
    const { user_id, task_id } = body; 
    const id = parseInt(user_id);

    // 1. Input Validation and User Check
    if (!task_id) {
        return sendError(res, 'Missing task_id in the request body.', 400);
    }

    try {
        const users = await supabaseFetch('users', 'GET', null, `?id=eq.${id}&select=id,balance,is_banned`);
        if (!ArrayOfUsers(users)) {
            return sendError(res, 'User not found.', 404);
        }
        const user = users[0];
        if (user.is_banned) {
            return sendError(res, 'User is banned.', 403);
        }

        // 2. Rate Limit Check 
        const rateLimitResult = await checkRateLimit(id);
        if (!rateLimitResult.ok) {
            return sendError(res, rateLimitResult.message, 429);
        }
        
        // 3. Check if user already completed this specific task
        const existingCompletion = await supabaseFetch('user_tasks', 'GET', null, `?user_id=eq.${id}&task_id=eq.${task_id}&select=id`);
        if (ArrayOfUsers(existingCompletion)) {
            return sendError(res, 'Task already completed by this user.', 409);
        }

        // 4. Fetch Task details and Check Global Limit
        const tasks = await supabaseFetch('dynamic_tasks', 'GET', null, `?id=eq.${task_id}&select=id,reward,max_users,completed_users,link,is_active`);
        if (!ArrayOfUsers(tasks) || !tasks[0].is_active) {
            return sendError(res, 'Invalid or inactive task ID.', 404);
        }

        const task = tasks[0];
        const reward = parseFloat(task.reward);
        
        if (task.completed_users >= task.max_users) {
            // Optional: Set task as inactive if limit reached
            await supabaseFetch('dynamic_tasks', 'PATCH', { is_active: false }, `?id=eq.${task_id}`);
            return sendError(res, 'Task has reached its maximum completion limit.', 403);
        }
        
        // 5. Verification Step: (يمكنك إضافة التحقق من الاشتراك في قناة تليجرام هنا باستخدام checkChannelMembership إذا كان الرابط رابط قناة)
        /*
        if (task.link && task.link.startsWith('https://t.me/')) {
            const channelUsername = task.link.split('/').pop();
            const isMember = await checkChannelMembership(id, channelUsername);
            if (!isMember) {
                return sendError(res, `Verification failed. You must join the channel: ${channelUsername}`, 400);
            }
        }
        */
        
        // 6. Process Reward and Update User Balance
        const newBalance = user.balance + reward;
        const updatePayload = { 
            balance: newBalance,
            // last_activity already updated in checkRateLimit
        };

        await supabaseFetch('users', 'PATCH', updatePayload, `?id=eq.${id}`);

        // 7. Track Task Completion for the User
        await supabaseFetch('user_tasks', 'POST', { 
            user_id: id, 
            task_id: task_id 
        });

        // 8. Increment Global Task Completion Counter
        await supabaseFetch('dynamic_tasks', 'PATCH', 
            { completed_users: task.completed_users + 1 }, 
            `?id=eq.${task_id}`
        );
        
        // 9. Success
        sendSuccess(res, { 
            new_balance: newBalance, 
            actual_reward: reward, 
            message: 'Task completed successfully.' 
        });

    } catch (error) {
        console.error('handleCompleteTask failed:', error.message);
        sendError(res, `Failed to complete task: ${error.message}`, 500);
    }
}


module.exports = async (req, res) => {
    if (req.method !== 'POST') {
        return sendError(res, 'Only POST requests are supported.', 405);
    }
    
    try {
        // 1. Read the entire request body
        const body = await new Promise((resolve, reject) => {
            let data = '';
            req.on('data', chunk => {
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

        if (!body || !body.type) {
            return sendError(res, 'Missing "type" field in the request body.', 400);
        }

        // ⬅️ initData Security Check: Bypass for 'commission' (which is just rate query)
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
            case 'getTaskData': // ⬅️ NEW: Get Task Data
                await handleGetTaskData(req, res, body);
                break;
            case 'completeTask': // ⬅️ UPDATED: Complete Dynamic Task
                await handleCompleteTask(req, res, body);
                break;
            default:
                sendError(res, `Unknown request type: ${body.type}`, 400);
        }
        
    } catch (error) {
        // Handle all exceptions caught during execution
        const statusCode = error.message.includes('401') ? 401 : 500;
        sendError(res, `Internal Server Error: ${error.message}`, statusCode);
    }
};
