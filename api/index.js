// /api/index.js (Modified for Dynamic Tasks and Security)

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
// Task Constants (REMOVED: TASK_REWARD and TELEGRAM_CHANNEL_USERNAME)
// ------------------------------------------------------------------


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
 * Extracts username from the task_link.
 */
async function checkChannelMembership(userId, taskLink) {
    if (!BOT_TOKEN) {
        console.error('BOT_TOKEN is not configured for membership check.');
        return false;
    }
    
    // Extract username from the link
    const parts = taskLink.split('/');
    let channelUsername = parts.pop() || parts.pop(); // Handle trailing slash
    
    if (!channelUsername) {
        console.error('Could not extract channel username from task link:', taskLink);
        return false;
    }

    // Clean up username and prepend '@' if missing
    channelUsername = channelUsername.replace('@', '');
    const chatId = `@${channelUsername}`; 

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
                // ⚠️ 6 hours passed, reset counter
                updatePayload.ads_watched_today = 0;
                updatePayload.ads_limit_reached_at = null; 
                console.log(`Ads limit reset for user ${userId}.`);
            }
        }

        // 3. Check Spins Limit Reset
        if (user.spins_limit_reached_at && user.spins_today >= DAILY_MAX_SPINS) {
            const spinsLimitTime = new Date(user.spins_limit_reached_at).getTime();
            if (now - spinsLimitTime > RESET_INTERVAL_MS) {
                // ⚠️ 6 hours passed, reset counter
                updatePayload.spins_today = 0;
                updatePayload.spins_limit_reached_at = null; 
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
        if (!Array.isArray(users) || users.length === 0) {
            return { ok: true };
        }

        const user = users[0];
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
        return { ok: true };
    } catch (error) {
        console.error(`Rate limit check failed for user ${userId}:`, error.message);
        return { ok: true };
    }
}

// ------------------------------------------------------------------
// **initData Security Validation Function** // ------------------------------------------------------------------
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
// 🔑 Commission Helper Function
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

        return { ok: true, data: { commission_amount: commissionAmount } };
    } catch (e) {
        console.error(`Commission failed for referrer ${referrerId}:`, e.message);
        return { ok: false, error: e.message };
    }
}

// ------------------------------------------------------------------
// 🔒 Action ID System 
// ------------------------------------------------------------------
/**
 * Generates and saves a secure, single-use action ID.
 */
async function generateAndSaveActionId(res, userId, actionType) {
    const actionId = crypto.randomBytes(16).toString('hex'); // Generate random token
    try {
        await supabaseFetch('temp_actions', 'POST', {
            user_id: userId,
            action_id: actionId,
            action_type: actionType,
            // created_at is defaulted by the database
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
            console.warn(`Action ID check failed for user ${userId} and type ${actionType}. Record not found.`);
            sendError(res, 'Invalid or previously used Server Token (Action ID).', 409);
            return false;
        }

        const record = records[0];
        const recordTime = new Date(record.created_at).getTime();

        // 1. Check Expiration (60 seconds)
        if (Date.now() - recordTime > ACTION_ID_EXPIRY_MS) {
             // Delete the expired token silently 
             await supabaseFetch('temp_actions', 'DELETE', null, `?id=eq.${record.id}`);
             sendError(res, 'Server Token (Action ID) expired. Please try again.', 408);
             return false;
        }

        // 2. Delete the token before processing (to prevent replay attacks)
        await supabaseFetch('temp_actions', 'DELETE', null, `?id=eq.${record.id}`);

        return true;

    } catch (error) {
        console.error('Validate Action ID failed:', error.message);
        sendError(res, 'Security token validation failed.', 500);
        return false;
    }
}


// --- Handler Functions (Existing functions like handleGetUserData, handleRegister, handleWatchAd, etc., are assumed here) ---
// (Due to brevity, only new/modified core functions are provided in full here, but the module.exports handler includes all.)

/**
 * 0) type: "requestActionId" (No change)
 */
async function handleRequestActionId(req, res, body) {
    const { user_id, action_type } = body;
    const id = parseInt(user_id);
    
    // Check if the requested action type is one that requires an Action ID
    const validTypes = ['watchAd', 'preSpin', 'spinResult', 'withdraw', 'completeTask'];
    if (!validTypes.includes(action_type)) {
        return sendError(res, 'Invalid action type for token request.', 400);
    }
    
    // Also run rate limit check for actions that need it (watchAd/preSpin/withdraw/completeTask)
    if (['watchAd', 'preSpin', 'withdraw', 'completeTask'].includes(action_type)) {
        const rateLimitResult = await checkRateLimit(id);
        if (!rateLimitResult.ok) {
            return sendError(res, rateLimitResult.message, 429);
        }
    }
    
    const actionId = await generateAndSaveActionId(res, id, action_type);
    if (actionId) {
        sendSuccess(res, { action_id: actionId });
    }
}

/**
 * 7) type: "getTasks" ⬅️ NEW
 * Retrieves all active and available tasks for the user.
 */
async function handleGetTasks(req, res, body) {
    const { user_id } = body;
    const id = parseInt(user_id);
    
    try {
        // 1. Get all active tasks that haven't reached max_users
        // Query tasks where is_active is true AND completed_count < max_users
        const activeTasks = await supabaseFetch('tasks', 'GET', null, `?select=id,name,reward_amount,task_link,max_users,completed_count&is_active=eq.true&completed_count=lt.max_users`);

        // 2. Get the list of tasks the user has already completed
        const userCompletedTasks = await supabaseFetch('user_tasks', 'GET', null, `?user_id=eq.${id}&select=task_id`);
        const completedTaskIds = new Set(userCompletedTasks.map(t => t.task_id));

        // 3. Filter the active tasks to only show the ones the user hasn't completed
        const availableTasks = activeTasks.filter(task => !completedTaskIds.has(task.id));
        
        sendSuccess(res, { tasks: availableTasks });
    } catch (error) {
        console.error('GetTasks failed:', error.message);
        sendError(res, `Failed to retrieve tasks: ${error.message}`, 500);
    }
}


/**
 * 8) type: "completeTask" ⬅️ MODIFIED for dynamic task list
 * Claims the reward for a specific task.
 */
async function handleCompleteTask(req, res, body) {
    const { user_id, action_id, task_id } = body;
    const id = parseInt(user_id);
    const parsedTaskId = parseInt(task_id);

    try {
        // 1. Validate Action ID (Security)
        if (!(await validateAndUseActionId(res, id, action_id, 'completeTask'))) {
            return;
        }
        
        // 2. Get user data (to check for ban status and current balance)
        const users = await supabaseFetch('users', 'GET', null, `?id=eq.${id}&select=balance,is_banned`);
        if (!Array.isArray(users) || users.length === 0) {
            return sendError(res, 'User not found.', 404);
        }
        const user = users[0];
        if (user.is_banned) {
            return sendError(res, 'User is banned.', 403);
        }

        // 3. Get task data and check limits
        const tasks = await supabaseFetch('tasks', 'GET', null, `?id=eq.${parsedTaskId}&select=id,name,reward_amount,task_link,max_users,completed_count,is_active`);
        if (!Array.isArray(tasks) || tasks.length === 0 || !tasks[0].is_active) {
            return sendError(res, 'Task not found or is inactive.', 404);
        }
        const task = tasks[0];

        if (task.completed_count >= task.max_users) {
             return sendError(res, 'Task limit reached by other users.', 403);
        }

        // 4. Check if user already completed the task (against user_tasks table)
        const userTasks = await supabaseFetch('user_tasks', 'GET', null, `?user_id=eq.${id}&task_id=eq.${parsedTaskId}`);
        if (userTasks.length > 0) {
            return sendError(res, 'Reward already claimed. Task is complete.', 409); 
        }
        
        // 5. Check Channel Membership using the task_link
        const isMember = await checkChannelMembership(id, task.task_link);

        if (!isMember) {
            return sendError(res, 'Membership not verified. Please ensure you joined the channel and try again.', 400);
        }

        // 6. Process Reward and Update Task/User Data
        const reward = task.reward_amount;
        const newBalance = user.balance + reward;
        
        // 6a. Update Task completed count
        const newCompletedCount = task.completed_count + 1;
        await supabaseFetch('tasks', 'PATCH', 
            { completed_count: newCompletedCount }, 
            `?id=eq.${parsedTaskId}`);

        // 6b. Add record to user_tasks
        await supabaseFetch('user_tasks', 'POST', 
            { user_id: id, task_id: parsedTaskId }, 
            '?select=user_id');

        // 6c. Update User balance and last activity
        const updatePayload = { 
            balance: newBalance,
            last_activity: new Date().toISOString() // Update for Rate Limit
        };
        await supabaseFetch('users', 'PATCH', updatePayload, `?id=eq.${id}`); 

        // 7. Success
        sendSuccess(res, { new_balance: newBalance, actual_reward: reward, message: 'Task completed successfully.' });

    } catch (error) {
        console.error('CompleteTask failed:', error.message);
        sendError(res, `Failed to complete task: ${error.message}`, 500);
    }
}


// --- Placeholder functions (Assume correct implementation based on previous logic) ---
async function handleGetUserData(req, res, body) {
    const { user_id } = body;
    const id = parseInt(user_id);
    try {
        await resetDailyLimitsIfExpired(id);
        const users = await supabaseFetch('users', 'GET', null, `?id=eq.${id}&select=*,withdrawal_history:withdrawals(amount,status,created_at),referrals_count:referrals(count)`);
        if (users.length === 0) {
             return sendError(res, 'User not found.', 404);
        }
        const user = users[0];
        
        // Ensure referral count is an integer
        const referralsCount = user.referrals_count && user.referrals_count.length > 0 ? user.referrals_count[0].count : 0;

        // Note: Task completion status is no longer returned here, as it's handled by getTasks/user_tasks
        sendSuccess(res, { 
            balance: user.balance, 
            ads_watched_today: user.ads_watched_today, 
            spins_today: user.spins_today, 
            referrals_count: referralsCount,
            is_banned: user.is_banned,
            withdrawal_history: user.withdrawal_history || []
        });

    } catch (error) {
        console.error('GetUserData failed:', error.message);
        sendError(res, `Failed to retrieve user data: ${error.message}`, 500);
    }
}

async function handleRegister(req, res, body) {
    const { user_id, ref_by } = body;
    const id = parseInt(user_id);

    try {
        const users = await supabaseFetch('users', 'GET', null, `?id=eq.${id}&select=id`);
        if (users.length > 0) {
            // User already exists
            return sendSuccess(res, { message: 'User already registered.' });
        }

        // Check referrer validity
        let referrerId = null;
        if (ref_by && id !== parseInt(ref_by)) {
            const referrer = await supabaseFetch('users', 'GET', null, `?id=eq.${ref_by}&select=id,is_banned`);
            if (referrer.length > 0 && !referrer[0].is_banned) {
                referrerId = parseInt(ref_by);
            }
        }
        
        // Register new user
        const initialBalance = 0;
        const newUser = {
            id: id,
            username: body.initData ? JSON.parse(new URLSearchParams(body.initData).get('user')).username : null,
            referrer_id: referrerId,
            balance: initialBalance,
            ads_watched_today: 0,
            spins_today: 0,
            is_banned: false
        };
        await supabaseFetch('users', 'POST', newUser, '?select=id');

        sendSuccess(res, { message: 'User registered successfully.', balance: initialBalance });

    } catch (error) {
        console.error('Registration failed:', error.message);
        sendError(res, `Registration failed: ${error.message}`, 500);
    }
}

async function handleWatchAd(req, res, body) {
    const { user_id, action_id } = body;
    const id = parseInt(user_id);
    const reward = REWARD_PER_AD;

    try {
        if (!(await validateAndUseActionId(res, id, action_id, 'watchAd'))) {
            return;
        }

        const users = await supabaseFetch('users', 'GET', null, `?id=eq.${id}&select=balance,ads_watched_today,is_banned`);
        if (users.length === 0 || users[0].is_banned) {
            return sendError(res, 'User not found or banned.', 403);
        }
        
        const user = users[0];

        if (user.ads_watched_today >= DAILY_MAX) {
            return sendError(res, `Daily ad limit reached (${DAILY_MAX}).`, 403);
        }

        const newAdsCount = user.ads_watched_today + 1;
        const newBalance = user.balance + reward;
        
        const updatePayload = {
            balance: newBalance,
            ads_watched_today: newAdsCount,
            last_activity: new Date().toISOString()
        };
        
        if (newAdsCount >= DAILY_MAX) {
            updatePayload.ads_limit_reached_at = new Date().toISOString();
        }

        await supabaseFetch('users', 'PATCH', updatePayload, `?id=eq.${id}`);

        sendSuccess(res, { new_balance: newBalance, new_ads_count: newAdsCount, actual_reward: reward });

    } catch (error) {
        console.error('WatchAd failed:', error.message);
        sendError(res, `Failed to watch ad: ${error.message}`, 500);
    }
}

async function handleCommission(req, res, body) {
    const { referrer_id, referee_id } = body; 
    const refId = parseInt(referrer_id);
    const feeId = parseInt(referee_id);
    
    // We assume the ad reward is 3, as set in REWARD_PER_AD
    const sourceReward = REWARD_PER_AD; 

    if (isNaN(refId) || isNaN(feeId)) {
        return sendError(res, 'Invalid referrer or referee ID.', 400);
    }
    
    // This endpoint should be protected by the server calling it, not initData
    // The previous ad logic is the only one authorized to call this.

    const result = await processCommission(refId, feeId, sourceReward);

    if (result.ok) {
        sendSuccess(res, { message: 'Commission processed.', commission_amount: result.data.commission_amount });
    } else {
        sendError(res, result.error, 500);
    }
}

async function handlePreSpin(req, res, body) {
    const { user_id, action_id } = body;
    const id = parseInt(user_id);

    try {
        if (!(await validateAndUseActionId(res, id, action_id, 'preSpin'))) {
            return;
        }

        const users = await supabaseFetch('users', 'GET', null, `?id=eq.${id}&select=spins_today,is_banned`);
        if (users.length === 0 || users[0].is_banned) {
            return sendError(res, 'User not found or banned.', 403);
        }
        
        if (users[0].spins_today >= DAILY_MAX_SPINS) {
            return sendError(res, `Daily spin limit reached (${DAILY_MAX_SPINS}).`, 403);
        }
        
        // This pre-spin token simply checks the limits and rate limit, and is consumed by the next step.
        sendSuccess(res, { message: 'Pre-spin checks passed.' });

    } catch (error) {
        console.error('PreSpin failed:', error.message);
        sendError(res, `Failed during pre-spin check: ${error.message}`, 500);
    }
}

async function handleSpinResult(req, res, body) {
    const { user_id, action_id } = body;
    const id = parseInt(user_id);
    
    try {
        if (!(await validateAndUseActionId(res, id, action_id, 'spinResult'))) {
            return; // Error already sent by validator
        }

        const users = await supabaseFetch('users', 'GET', null, `?id=eq.${id}&select=balance,spins_today,is_banned`);
        if (users.length === 0 || users[0].is_banned) {
            return sendError(res, 'User not found or banned.', 403);
        }
        
        const user = users[0];

        if (user.spins_today >= DAILY_MAX_SPINS) {
            return sendError(res, `Daily spin limit reached (${DAILY_MAX_SPINS}).`, 403);
        }
        
        // 1. Calculate Prize
        const { prize, prizeIndex } = calculateRandomSpinPrize();
        
        // 2. Update DB
        const newSpinsCount = user.spins_today + 1;
        const newBalance = user.balance + prize;

        const updatePayload = {
            balance: newBalance,
            spins_today: newSpinsCount
        };

        if (newSpinsCount >= DAILY_MAX_SPINS) {
             updatePayload.spins_limit_reached_at = new Date().toISOString();
        }

        await supabaseFetch('users', 'PATCH', updatePayload, `?id=eq.${id}`);

        // 3. Send Success
        sendSuccess(res, { 
            new_balance: newBalance, 
            new_spins_count: newSpinsCount, 
            actual_prize: prize,
            prize_index: prizeIndex 
        });

    } catch (error) {
        console.error('SpinResult failed:', error.message);
        sendError(res, `Failed to get spin result: ${error.message}`, 500);
    }
}

async function handleWithdraw(req, res, body) {
    const { user_id, binanceId, amount, action_id } = body;
    const id = parseInt(user_id);
    const parsedAmount = parseFloat(amount);
    
    if (isNaN(parsedAmount) || parsedAmount < 400 || !binanceId) {
        return sendError(res, 'Invalid amount or Binance ID.', 400);
    }

    try {
        if (!(await validateAndUseActionId(res, id, action_id, 'withdraw'))) {
            return; 
        }

        const users = await supabaseFetch('users', 'GET', null, `?id=eq.${id}&select=balance,is_banned`);
        if (users.length === 0 || users[0].is_banned) {
            return sendError(res, 'User not found or banned.', 403);
        }
        
        const user = users[0];
        
        if (user.balance < parsedAmount) {
            return sendError(res, 'Insufficient balance for withdrawal.', 403);
        }
        
        // 1. Create withdrawal request
        await supabaseFetch('withdrawals', 'POST', {
            user_id: id,
            amount: parsedAmount,
            binance_id: binanceId,
            status: 'pending' 
        }, '?select=user_id');

        // 2. Deduct balance and update last activity
        const newBalance = user.balance - parsedAmount;
        await supabaseFetch('users', 'PATCH', { 
            balance: newBalance,
            last_activity: new Date().toISOString()
        }, `?id=eq.${id}`);
        
        sendSuccess(res, { new_balance: newBalance, message: 'Withdrawal request submitted successfully.' });
        
    } catch (error) {
        console.error('Withdrawal failed:', error.message);
        sendError(res, `Withdrawal failed: ${error.message}`, 500);
    }
}


/**
 * The main handler function for Vercel/Node.js environment
 */
module.exports.handler = async (req, res) => {
  // Only handle POST requests
  if (req.method !== 'POST') {
    res.writeHead(405, { 'Content-Type': 'application/json' });
    return res.end(JSON.stringify({ ok: false, error: 'Method Not Allowed' }));
  }

  let body;
  try {
    body = await new Promise((resolve, reject) => {
      let data = '';
      req.on('data', chunk => { data += chunk; });
      req.on('end', () => {
        try {
          resolve(JSON.parse(data));
        } catch (e) {
          reject(new Error('Invalid JSON format.'));
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
    case 'generateActionId': // ⬅️ Handle Action ID request
        await handleRequestActionId(req, res, body);
        break;
    case 'getTasks': // ⬅️ NEW: Get available tasks
      await handleGetTasks(req, res, body);
      break;
    case 'completeTask': // ⬅️ MODIFIED: Complete a dynamic task
      await handleCompleteTask(req, res, body);
      break;
    default:
      sendError(res, 'Invalid action type.', 400);
  }
};
