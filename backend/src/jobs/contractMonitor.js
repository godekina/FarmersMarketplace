/**
 * jobs/contractMonitor.js
 *
 * Polls Soroban contract events every 5 minutes.
 * Detects:
 *   - 3+ failed invocations within the last hour → alert type: failed_invocations
 *   - Any transfer > 1000 XLM                   → alert type: large_transfer
 *
 * Creates a contract_alerts row and emails the admin on each new alert.
 * Also sends push notifications to admin for failed_invocations alerts (#698).
 * Implements exponential backoff retry on RPC failures (max 5 retries, cap 5 minutes).
 */

const db = require('../db/schema');
const { getContractEvents } = require('../utils/stellar');
const mailer = require('../utils/mailer');
const { sendPushToUser } = require('../utils/pushNotifications');
const logger = require('../logger');

const POLL_INTERVAL_MS = 5 * 60 * 1000; // 5 minutes
const FAILED_INVOCATION_THRESHOLD = 3;
const LARGE_TRANSFER_XLM = 1000;
const MAX_RETRIES = 5;
const MAX_BACKOFF_MS = 5 * 60 * 1000; // 5 minutes

async function getAdminEmail() {
  const { rows } = await db.query(
    `SELECT email FROM users WHERE role = 'admin' LIMIT 1`
  );
  return rows[0]?.email || null;
}

async function getAdminId() {
  const { rows } = await db.query(
    `SELECT id FROM users WHERE role = 'admin' LIMIT 1`
  );
  return rows[0]?.id || null;
}

async function createAlert(contract_id, alert_type, message) {
  // Avoid duplicate alerts: skip if same contract+type alert exists in last 5 min
  const { rows } = await db.query(
    `SELECT id FROM contract_alerts
     WHERE contract_id = $1 AND alert_type = $2
       AND created_at >= datetime('now', '-5 minutes')
     LIMIT 1`,
    [contract_id, alert_type]
  );
  if (rows.length) return null;

  const { rows: inserted } = await db.query(
    `INSERT INTO contract_alerts (contract_id, alert_type, message)
     VALUES ($1, $2, $3) RETURNING *`,
    [contract_id, alert_type, message]
  );

  const alert = inserted[0];

  // Email admin
  const adminEmail = await getAdminEmail();
  if (adminEmail) {
    await mailer.sendContractAlert({ to: adminEmail, alert }).catch((e) =>
      logger.error('[ContractMonitor] Email failed:', e.message)
    );
  }

  // #698 — push notification for failed invocations and monitored events
  if (alert_type === 'failed_invocations' || alert_type === 'contract_event') {
    const adminId = await getAdminId();
    if (adminId) {
      await sendPushToUser(adminId, {
        title: 'Contract Alert',
        body: message,
        data: { alert_type, contract_id },
      }).catch((e) =>
        logger.error('[ContractMonitor] Push notification failed:', e.message)
      );
    }
  }

  return alert;
}

async function monitorContract(contractId, retryCount = 0) {
  const oneHourAgo = new Date(Date.now() - 60 * 60 * 1000).toISOString();

  let result;
  try {
    result = await getContractEvents(contractId, { from: oneHourAgo, limit: 200 });
  } catch (err) {
    // Implement exponential backoff retry
    if (retryCount < MAX_RETRIES) {
      const backoffMs = Math.min(
        Math.pow(2, retryCount) * 1000, // exponential: 1s, 2s, 4s, 8s, 16s
        MAX_BACKOFF_MS
      );
      logger.warn(
        `[ContractMonitor] Failed to fetch events for ${contractId}, retrying in ${backoffMs}ms (attempt ${retryCount + 1}/${MAX_RETRIES}):`,
        err.message
      );
      await new Promise((resolve) => setTimeout(resolve, backoffMs));
      return monitorContract(contractId, retryCount + 1);
    }

    // Exhausted retries
    logger.error(
      `[ContractMonitor] Failed to fetch events for ${contractId} after ${MAX_RETRIES} retries:`,
      err.message
    );

    // Send admin notification
    try {
      const { rows } = await db.query(
        `SELECT email FROM users WHERE role = 'admin' LIMIT 1`
      );
      const adminEmail = rows[0]?.email;
      if (adminEmail) {
        await mailer.sendContractAlert({
          to: adminEmail,
          alert: {
            alert_type: 'monitor_failure',
            contract_id: contractId,
            message: `Contract monitor failed after ${MAX_RETRIES} retries. RPC may be unavailable. Error: ${err.message}`,
            created_at: new Date().toISOString(),
          },
        }).catch((e) =>
          logger.error('[ContractMonitor] Failed to send admin notification:', e.message)
        );
      }
    } catch (notifyErr) {
      logger.error('[ContractMonitor] Error sending admin notification:', notifyErr.message);
    }

    return;
  }

  const events = result.events || [];

  // Detect failed invocations
  const failures = events.filter((e) => {
    const topics = e.topics || [];
    return topics.some(
      (t) => typeof t === 'string' && /fail|error|revert/i.test(t)
    ) || e.type === 'diagnostic';
  });

  if (failures.length >= FAILED_INVOCATION_THRESHOLD) {
    await createAlert(
      contractId,
      'failed_invocations',
      `${failures.length} failed invocations detected in the last hour for contract ${contractId}`
    );
  }

  // Detect large transfers
  for (const ev of events) {
    const topics = ev.topics || [];
    const isTransfer = topics.some(
      (t) => typeof t === 'string' && /transfer/i.test(t)
    );
    if (!isTransfer) continue;

    // data may be the amount (native XLM in stroops or XLM directly)
    let amount = null;
    if (typeof ev.data === 'bigint' || typeof ev.data === 'number') {
      amount = Number(ev.data);
      // If in stroops (1 XLM = 10_000_000 stroops)
      if (amount > 1e10) amount = amount / 1e7;
    } else if (typeof ev.data === 'object' && ev.data !== null) {
      const val = ev.data.amount ?? ev.data.value ?? ev.data;
      amount = typeof val === 'bigint' ? Number(val) / 1e7 : parseFloat(val) || null;
    }

    if (amount !== null && amount > LARGE_TRANSFER_XLM) {
      await createAlert(
        contractId,
        'large_transfer',
        `Large transfer of ${amount.toFixed(2)} XLM detected on contract ${contractId} (ledger ${ev.ledger})`
      );
    }
  }
}

/**
 * #698 — Query contract_alerts for unacknowledged failed_invocations and
 * contract_event rows created in the last poll window, and send push
 * notifications to the admin for each one.
 */
async function processUnacknowledgedAlerts() {
  try {
    const { rows: alerts } = await db.query(
      `SELECT id, contract_id, alert_type, message
       FROM contract_alerts
       WHERE acknowledged = 0
         AND alert_type IN ('failed_invocations', 'contract_event')
       ORDER BY created_at DESC
       LIMIT 50`
    );

    if (!alerts.length) return;

    const adminId = await getAdminId();
    if (!adminId) return;

    for (const alert of alerts) {
      await sendPushToUser(adminId, {
        title: 'Contract Alert',
        body: alert.message,
        data: { alert_type: alert.alert_type, contract_id: alert.contract_id },
      }).catch((e) =>
        logger.error('[ContractMonitor] Push failed for alert', alert.id, e.message)
      );
    }
  } catch (err) {
    logger.error('[ContractMonitor] processUnacknowledgedAlerts error:', err.message);
  }
}

async function runMonitoringJob() {
  try {
    const { rows: contracts } = await db.query(
      `SELECT contract_id FROM contracts_registry`
    );
    await Promise.all(contracts.map((c) => monitorContract(c.contract_id)));

    // #698 — query contract_alerts for any unacknowledged alerts and push-notify admin
    await processUnacknowledgedAlerts();
  } catch (err) {
    logger.error('[ContractMonitor] Job error:', err.message);
  }
}

function startContractMonitor() {
  logger.info('[ContractMonitor] Starting — polling every 5 minutes');
  runMonitoringJob(); // run immediately on startup
  return setInterval(runMonitoringJob, POLL_INTERVAL_MS);
}

module.exports = { startContractMonitor, runMonitoringJob };

// .
