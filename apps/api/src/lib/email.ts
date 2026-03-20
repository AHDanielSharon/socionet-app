import nodemailer, { Transporter } from 'nodemailer';
import { config } from '@config/index';
import { logger } from '@utils/logger';

let transporter: Transporter;

export const initEmail = (): void => {
  transporter = nodemailer.createTransport({
    host: config.email.host,
    port: config.email.port,
    secure: config.email.secure,
    auth: config.email.user ? {
      user: config.email.user,
      pass: config.email.pass,
    } : undefined,
    pool: true,
    maxConnections: 5,
    maxMessages: 100,
  });

  transporter.verify((err) => {
    if (err) logger.warn('Email transport not ready', { error: err.message });
    else logger.info('Email transport ready');
  });
};

const baseTemplate = (content: string): string => `
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>SOCIONET</title>
</head>
<body style="margin:0;padding:0;background:#060608;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;-webkit-font-smoothing:antialiased">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#060608;padding:40px 16px">
    <tr><td align="center">
      <table width="540" cellpadding="0" cellspacing="0" style="max-width:540px;width:100%;background:#16161f;border:1px solid #2a2a3a;border-radius:20px;overflow:hidden">
        <!-- Header -->
        <tr>
          <td style="background:linear-gradient(135deg,#1a0533,#062040);padding:32px;text-align:center;border-bottom:1px solid #2a2a3a">
            <table cellpadding="0" cellspacing="0" style="margin:0 auto">
              <tr>
                <td style="width:52px;height:52px;background:linear-gradient(135deg,#7c6af7,#00f5d4);border-radius:16px;text-align:center;vertical-align:middle;font-size:24px;font-weight:900;color:#fff;font-family:Georgia,serif">S</td>
                <td style="padding-left:12px;text-align:left">
                  <div style="color:#f0f0fa;font-size:22px;font-weight:900;letter-spacing:-0.5px">SOCIONET</div>
                  <div style="color:#6b6b85;font-size:11px;letter-spacing:3px;text-transform:uppercase;margin-top:2px">THE FUTURE OF SOCIAL</div>
                </td>
              </tr>
            </table>
          </td>
        </tr>
        <!-- Content -->
        <tr><td style="padding:36px 40px">${content}</td></tr>
        <!-- Footer -->
        <tr>
          <td style="background:#0d0d12;padding:20px 40px;border-top:1px solid #2a2a3a;text-align:center">
            <p style="margin:0;color:#3a3a52;font-size:12px;line-height:1.6">
              © ${new Date().getFullYear()} SOCIONET &bull; Decentralized &bull; Private &bull; Yours<br>
              <a href="${config.app.url}" style="color:#7c6af7;text-decoration:none">${config.app.url}</a>
            </p>
          </td>
        </tr>
      </table>
    </td></tr>
  </table>
</body>
</html>`;

const send = async (to: string, subject: string, html: string): Promise<void> => {
  if (!config.email.user) {
    logger.warn('Email not configured — skipping send', { to, subject });
    return;
  }
  try {
    await transporter.sendMail({ from: config.email.from, to, subject, html });
    logger.debug('Email sent', { to, subject });
  } catch (err) {
    logger.error('Email send failed', { to, subject, error: String(err) });
    throw err;
  }
};

// ── OTP Email
export const sendOTPEmail = async (email: string, otp: string, purpose: string): Promise<void> => {
  const titles: Record<string, string> = {
    register: 'Welcome to SOCIONET — Verify your email',
    login: 'Your SOCIONET login code',
    reset_password: 'Reset your SOCIONET password',
    verify: 'Verify your SOCIONET email',
  };

  const descs: Record<string, string> = {
    register: 'Use this code to complete your account setup.',
    login: 'Use this code to sign in to your account.',
    reset_password: 'Use this code to reset your password. Never share it.',
    verify: 'Use this code to verify your email address.',
  };

  const content = `
    <h2 style="margin:0 0 12px;color:#f0f0fa;font-size:22px;font-weight:700">Your Verification Code</h2>
    <p style="margin:0 0 28px;color:#b0b0c8;font-size:15px;line-height:1.6">${descs[purpose] || 'Use this code to continue.'}</p>
    <div style="background:#0d0d12;border:1px solid #2a2a3a;border-radius:16px;padding:28px;text-align:center;margin-bottom:28px">
      <div style="font-size:44px;font-weight:800;letter-spacing:14px;color:#7c6af7;font-family:'Courier New',monospace;margin-bottom:8px">${otp}</div>
      <p style="margin:0;color:#6b6b85;font-size:13px">Expires in ${config.otp.expiryMinutes} minutes</p>
    </div>
    <div style="background:rgba(124,106,247,0.08);border:1px solid rgba(124,106,247,0.2);border-radius:12px;padding:16px">
      <p style="margin:0;color:#6b6b85;font-size:13px;line-height:1.6">
        🔒 <strong style="color:#b0b0c8">Never share this code.</strong> SOCIONET will never ask for it. 
        If you didn't request this, you can safely ignore this email.
      </p>
    </div>`;

  await send(email, titles[purpose] || 'SOCIONET Verification Code', baseTemplate(content));
};

// ── Welcome Email
export const sendWelcomeEmail = async (email: string, username: string, fullName: string): Promise<void> => {
  const content = `
    <h2 style="margin:0 0 8px;color:#f0f0fa;font-size:24px;font-weight:800">Welcome, ${fullName}! 🚀</h2>
    <p style="margin:0 0 24px;color:#b0b0c8;font-size:15px;line-height:1.7">
      You're now part of something different. SOCIONET is built with one goal: 
      giving social media back to the people. Your data is yours. Your identity is decentralized. Your privacy is absolute.
    </p>
    <div style="background:#0d0d12;border-radius:16px;padding:24px;margin-bottom:24px">
      <p style="margin:0 0 16px;color:#f0f0fa;font-weight:600;font-size:15px">Get started:</p>
      ${[
        ['📸', 'Complete your profile', 'Add a photo, bio, and website'],
        ['👥', 'Find your people', 'Follow creators and join communities'],
        ['✍️', 'Share your first post', 'Text, photo, video, or reel'],
        ['💬', 'Start a conversation', 'Private, encrypted, real-time'],
      ].map(([emoji, title, desc]) => `
        <div style="display:flex;align-items:flex-start;margin-bottom:12px">
          <span style="font-size:20px;margin-right:12px;flex-shrink:0">${emoji}</span>
          <div>
            <div style="color:#f0f0fa;font-size:14px;font-weight:600">${title}</div>
            <div style="color:#6b6b85;font-size:13px;margin-top:2px">${desc}</div>
          </div>
        </div>`).join('')}
    </div>
    <p style="margin:0 0 16px;color:#6b6b85;font-size:13px">Your username is: <strong style="color:#7c6af7;font-family:monospace">@${username}</strong></p>
    <div style="text-align:center;margin-top:28px">
      <a href="${config.app.url}" style="display:inline-block;background:linear-gradient(135deg,#7c6af7,#6d5ce7);color:#fff;text-decoration:none;padding:14px 36px;border-radius:12px;font-weight:700;font-size:15px">
        Open SOCIONET →
      </a>
    </div>`;

  await send(email, `Welcome to SOCIONET, @${username}! 🎉`, baseTemplate(content));
};

// ── Notification email digest
export const sendNotificationDigest = async (
  email: string,
  username: string,
  notifications: Array<{ title: string; body: string; time: string }>
): Promise<void> => {
  const notifRows = notifications.slice(0, 10).map(n => `
    <div style="border-bottom:1px solid #2a2a3a;padding:14px 0">
      <div style="color:#f0f0fa;font-size:14px;font-weight:600">${n.title}</div>
      <div style="color:#b0b0c8;font-size:13px;margin-top:4px">${n.body}</div>
      <div style="color:#6b6b85;font-size:11px;margin-top:4px">${n.time}</div>
    </div>`).join('');

  const content = `
    <h2 style="margin:0 0 8px;color:#f0f0fa;font-size:20px;font-weight:700">Your activity digest</h2>
    <p style="margin:0 0 24px;color:#6b6b85;font-size:14px">Here's what happened since you last visited, @${username}:</p>
    <div style="background:#0d0d12;border-radius:16px;overflow:hidden">${notifRows}</div>
    <div style="text-align:center;margin-top:24px">
      <a href="${config.app.url}/notifications" style="display:inline-block;background:linear-gradient(135deg,#7c6af7,#6d5ce7);color:#fff;text-decoration:none;padding:12px 28px;border-radius:10px;font-weight:600;font-size:14px">
        View All Notifications
      </a>
    </div>`;

  await send(email, `📬 Your SOCIONET digest`, baseTemplate(content));
};

// ── Password changed alert
export const sendPasswordChangedEmail = async (email: string, username: string): Promise<void> => {
  const content = `
    <h2 style="margin:0 0 16px;color:#f0f0fa;font-size:20px;font-weight:700">🔐 Password Changed</h2>
    <p style="margin:0 0 20px;color:#b0b0c8;font-size:15px;line-height:1.6">
      Hey @${username}, your SOCIONET password was just changed. If this was you, no action is needed.
    </p>
    <div style="background:rgba(255,77,109,0.08);border:1px solid rgba(255,77,109,0.2);border-radius:12px;padding:16px;margin-bottom:24px">
      <p style="margin:0;color:#ff4d6d;font-size:14px;font-weight:600">⚠️ Wasn't you?</p>
      <p style="margin:8px 0 0;color:#b0b0c8;font-size:13px">
        If you did not make this change, please immediately reset your password using the link below and contact our support team.
      </p>
    </div>
    <div style="text-align:center">
      <a href="${config.app.url}/auth/forgot-password" style="display:inline-block;background:#ff4d6d;color:#fff;text-decoration:none;padding:12px 28px;border-radius:10px;font-weight:600;font-size:14px">
        Secure My Account
      </a>
    </div>`;

  await send(email, '⚠️ SOCIONET — Password changed', baseTemplate(content));
};

// ── New device login alert
export const sendNewLoginAlert = async (
  email: string,
  username: string,
  device: string,
  ip: string,
  time: string
): Promise<void> => {
  const content = `
    <h2 style="margin:0 0 16px;color:#f0f0fa;font-size:20px;font-weight:700">New Login Detected</h2>
    <p style="margin:0 0 20px;color:#b0b0c8;font-size:15px">Your account @${username} was just accessed from a new device.</p>
    <div style="background:#0d0d12;border-radius:16px;padding:20px;margin-bottom:24px">
      ${[['Device', device], ['IP Address', ip], ['Time', time]].map(([label, value]) => `
        <div style="display:flex;justify-content:space-between;padding:8px 0;border-bottom:1px solid #2a2a3a">
          <span style="color:#6b6b85;font-size:13px">${label}</span>
          <span style="color:#f0f0fa;font-size:13px;font-family:monospace">${value}</span>
        </div>`).join('')}
    </div>
    <p style="margin:0 0 20px;color:#6b6b85;font-size:13px">If this was you, no action needed. Otherwise, secure your account immediately.</p>
    <div style="text-align:center">
      <a href="${config.app.url}/settings/security" style="display:inline-block;background:linear-gradient(135deg,#7c6af7,#6d5ce7);color:#fff;text-decoration:none;padding:12px 28px;border-radius:10px;font-weight:600;font-size:14px">
        Review Security Settings
      </a>
    </div>`;

  await send(email, '🔔 SOCIONET — New login to your account', baseTemplate(content));
};
