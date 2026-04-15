'use strict';

const axios = require('axios');
const fs = require('fs-extra');
const path = require('path');
const os = require('os');
const { spawn } = require('child_process');
const { google } = require('googleapis');
const cloudinary = require('cloudinary').v2;
const { v4: uuidv4 } = require('uuid');

// ---------------------------------------------------------------------------
// Cloudinary config
// ---------------------------------------------------------------------------
cloudinary.config({
  cloud_name: process.env.CLOUDINARY_CLOUD_NAME,
  api_key: process.env.CLOUDINARY_API_KEY,
  api_secret: process.env.CLOUDINARY_API_SECRET,
  secure: true,
});

// ---------------------------------------------------------------------------
// Helper – spawn a command and resolve/reject based on exit code
// ---------------------------------------------------------------------------
function run(cmd, args) {
  return new Promise((resolve, reject) => {
    console.log(`  [run] ${cmd} ${args.join(' ')}`);
    const p = spawn(cmd, args, { stdio: ['ignore', 'inherit', 'inherit'] });
    p.on('error', (err) => reject(new Error(`Failed to spawn '${cmd}': ${err.message}`)));
    p.on('close', (code) => {
      if (code === 0) resolve();
      else reject(new Error(`'${cmd}' exited with code ${code}`));
    });
  });
}

// ---------------------------------------------------------------------------
// 1. Generate quote via Groq Chat Completions (MAX 10 WORDS)
// ---------------------------------------------------------------------------
const PROMPT_MAP = {
  islamic:
    'Write ONE Islamic reminder, maximum 10 words. Output ONLY the text. No emojis. No quotes. No references.',
  motivation:
    'Write ONE motivational quote, maximum 10 words. Output ONLY the text. No emojis. No quotes.',
  success:
    'Write ONE success mindset quote, maximum 10 words. Output ONLY the text. No emojis. No quotes.',
};

function normalizeQuote(raw) {
  let q = String(raw || '')
    .replace(/^"+|"+$/g, '') // remove surrounding quotes
    .replace(/\s+/g, ' ')
    .trim();

  // hard limit: 10 words
  const words = q.split(' ').filter(Boolean);
  if (words.length > 10) q = words.slice(0, 10).join(' ');
  return q;
}

async function generateQuote(contentType) {
  const prompt = PROMPT_MAP[contentType] || PROMPT_MAP.motivation;

  const res = await axios.post(
    'https://api.groq.com/openai/v1/chat/completions',
    {
      model: 'llama-3.1-8b-instant',
      messages: [
        { role: 'system', content: 'You output only the final text. No extra formatting.' },
        { role: 'user', content: prompt },
      ],
      max_tokens: 40,
      temperature: 0.8,
    },
    {
      headers: {
        Authorization: `Bearer ${process.env.GROQ_API_KEY}`,
        'Content-Type': 'application/json',
      },
      timeout: 30000,
    }
  );

  const raw = res?.data?.choices?.[0]?.message?.content;
  const quote = normalizeQuote(raw);
  if (!quote) throw new Error('Groq returned empty quote');
  return quote;
}

// ---------------------------------------------------------------------------
// 2. Generate background image (solid color using ImageMagick)
// ---------------------------------------------------------------------------
const BG_COLOR_MAP = {
  paper: 'f5f0e8',
  dark: '1a1a2e',
  light: 'ffffff',
};

async function generateBackground(bgType, tmpDir) {
  const color = BG_COLOR_MAP[bgType] || BG_COLOR_MAP.dark;
  const bgPath = path.join(tmpDir, 'background.png');
  await run('convert', ['-size', '1080x1920', `xc:#${color}`, bgPath]);
  return bgPath;
}

// ---------------------------------------------------------------------------
// 3. Render quote text onto background (NO @file -> avoids IM security policy)
// ---------------------------------------------------------------------------
function wrapLines(text, maxLen = 18, maxLines = 5) {
  const words = String(text || '').replace(/\s+/g, ' ').trim().split(' ').filter(Boolean);
  if (words.length === 0) return [''];

  const lines = [];
  let line = '';

  for (const w of words) {
    const next = line ? (line + ' ' + w) : w;
    if (next.length <= maxLen) line = next;
    else {
      if (line) lines.push(line);
      line = w;
      if (lines.length >= maxLines - 1) break;
    }
  }

  if (line && lines.length < maxLines) lines.push(line);
  return lines;
}

function escapeForImagemagick(s) {
  // keep it simple: only escape backslash and double quotes
  return String(s).replace(/\\/g, '\\\\').replace(/"/g, '\\"');
}

async function renderTextOnImage(bgPath, quote, tmpDir) {
  const outputPath = path.join(tmpDir, 'overlay.png');

  const lines = wrapLines(quote, 18, 5);
  const text = escapeForImagemagick(lines.join('\n'));

  await run('convert', [
    bgPath,
    '-gravity', 'Center',
    '-font', 'DejaVu-Sans-Bold',
    '-pointsize', '78',
    '-interline-spacing', '14',

    // shadow
    '-fill', 'rgba(0,0,0,0.35)',
    '-annotate', '+3+3', text,

    // main
    '-fill', '#111827',
    '-annotate', '+0+0', text,

    outputPath,
  ]);

  return outputPath;
}

// ---------------------------------------------------------------------------
// 4. Upload overlay image to Cloudinary
// ---------------------------------------------------------------------------
async function uploadToCloudinary(filePath, publicId) {
  return cloudinary.uploader.upload(filePath, {
    public_id: publicId,
    resource_type: 'image',
    overwrite: true,
  });
}

// ---------------------------------------------------------------------------
// 5. Generate final 9:16 short video URL via Cloudinary transformations
// ---------------------------------------------------------------------------
const BASE_PUBLIC_ID = 'ai-reel-bot/base_template_v4';

async function generateShortVideo(overlayPublicId) {
  const VIDEO_SECONDS = 12;

  const videoUrl = cloudinary.url(BASE_PUBLIC_ID, {
    resource_type: 'video',
    transformation: [
      {
        width: 1080,
        height: 1920,
        crop: 'fill',
        duration: VIDEO_SECONDS,
        overlay: overlayPublicId.replace(/\//g, ':'),
      },
      { flags: 'layer_apply', gravity: 'center' },
      { quality: 'auto:best', fetch_format: 'mp4' },
    ],
  });

  return videoUrl;
}

// ---------------------------------------------------------------------------
// 6. Download video
// ---------------------------------------------------------------------------
async function downloadVideo(videoUrl, tmpDir) {
  if (!videoUrl) throw new Error('videoUrl is empty – cannot download');

  const videoPath = path.join(tmpDir, 'short.mp4');
  console.log('  [video] Downloading from Cloudinary…');

  const response = await axios.get(videoUrl, {
    responseType: 'stream',
    validateStatus: () => true,
  });

  if (response.status !== 200) {
    throw new Error(`Download failed with status ${response.status}: ${videoUrl}`);
  }

  const writer = fs.createWriteStream(videoPath);
  await new Promise((resolve, reject) => {
    response.data.pipe(writer);
    writer.on('finish', resolve);
    writer.on('error', reject);
  });

  return videoPath;
}

// ---------------------------------------------------------------------------
// 7. Upload video to YouTube (✅ FIXED TOKEN MAPPING)
// ---------------------------------------------------------------------------
async function uploadToYouTube(videoPath, title, youtubeTokens) {
  const clientId = process.env.GOOGLE_CLIENT_ID;
  const clientSecret = process.env.GOOGLE_CLIENT_SECRET;

  const refreshToken = youtubeTokens?.refresh_token;
  const accessToken = youtubeTokens?.access_token;

  const oauth2Client = new google.auth.OAuth2(clientId, clientSecret);

  if (refreshToken) {
    oauth2Client.setCredentials({ refresh_token: refreshToken });
  } else if (accessToken) {
    oauth2Client.setCredentials({ access_token: accessToken });
  } else {
    throw new Error('No valid YouTube credentials');
  }

  const youtube = google.youtube({ version: 'v3', auth: oauth2Client });

  const res = await youtube.videos.insert({
    part: ['snippet', 'status'],
    requestBody: {
      snippet: {
        title,
        description: 'Auto-generated YouTube Short',
        tags: ['Shorts', 'AI', 'automation'],
        categoryId: '22',
      },
      status: {
        privacyStatus: 'public',
        selfDeclaredMadeForKids: false,
      },
    },
    media: {
      body: fs.createReadStream(videoPath),
    },
  });

  return res.data;
}

// ---------------------------------------------------------------------------
// Main pipeline for a single user
// ---------------------------------------------------------------------------
async function processUser(user, tmpDir) {
  const { email, contentType = 'motivation', bgType = 'dark', youtubeTokens } = user;

  const jobId = uuidv4();
  const userTmpDir = path.join(tmpDir, jobId);
  await fs.ensureDir(userTmpDir);

  try {
    console.log(`\n[user] Processing: ${email} (contentType=${contentType}, bgType=${bgType})`);

    console.log('  [1/7] Generating quote…');
    const quote = await generateQuote(contentType);
    console.log(`  [quote] "${quote}"`);

    console.log('  [2/7] Generating background…');
    const bgPath = await generateBackground(bgType, userTmpDir);

    console.log('  [3/7] Rendering text on image…');
    const overlayPath = await renderTextOnImage(bgPath, quote, userTmpDir);

    const overlayPublicId = `ytworker/overlay_${jobId}`;
    console.log(`  [4/7] Uploading overlay to Cloudinary (${overlayPublicId})…`);
    await uploadToCloudinary(overlayPath, overlayPublicId);

    console.log('  [5/7] Generating video via Cloudinary…');
    const videoUrl = await generateShortVideo(overlayPublicId);
    console.log(`  [video url] ${videoUrl}`);

    console.log('  [6/7] Downloading video…');
    const videoPath = await downloadVideo(videoUrl, userTmpDir);

    console.log('  [7/7] Uploading to YouTube…');
    const videoTitle = `${quote} #Shorts`; // already <=10 words
    const ytResult = await uploadToYouTube(videoPath, videoTitle, youtubeTokens);

    console.log(`  [youtube] Uploaded! Video ID: ${ytResult.id}`);
  } finally {
    await fs.remove(userTmpDir).catch(() => {});
    console.log(`  [cleanup] Removed temp dir for ${email}`);
  }
}

// ---------------------------------------------------------------------------
// Main worker entry point
// ---------------------------------------------------------------------------
async function runWorker() {
  console.log('=== YTworker started at', new Date().toISOString(), '===');

  const required = [
    'GROQ_API_KEY',
    'CLOUDINARY_CLOUD_NAME',
    'CLOUDINARY_API_KEY',
    'CLOUDINARY_API_SECRET',
    'BACKEND_URL',
    'WORKER_SECRET',
    'GOOGLE_CLIENT_ID',
    'GOOGLE_CLIENT_SECRET',
  ];

  const missing = required.filter((k) => !process.env[k]);
  if (missing.length > 0) {
    console.error('[fatal] Missing required environment variables:', missing.join(', '));
    process.exit(1);
  }

  const tmpDir = path.join(os.tmpdir(), `ytworker_${Date.now()}`);
  await fs.ensureDir(tmpDir);

  const cleanup = async () => {
    await fs.remove(tmpDir).catch(() => {});
    console.log('[cleanup] Removed shared temp dir');
  };

  process.on('exit', () => {
    try {
      fs.removeSync(tmpDir);
    } catch (_) {}
  });

  process.on('SIGINT', () => cleanup().finally(() => process.exit(0)));
  process.on('SIGTERM', () => cleanup().finally(() => process.exit(0)));

  try {
    console.log(`[worker] Fetching users from ${process.env.BACKEND_URL}/worker/users`);
    const usersRes = await axios.get(`${process.env.BACKEND_URL}/worker/users`, {
      headers: { Authorization: `Bearer ${process.env.WORKER_SECRET}` },
      timeout: 30000,
    });

    const users = usersRes.data.users;
    if (!Array.isArray(users) || users.length === 0) {
      console.log('[worker] No users to process. Exiting.');
      return;
    }

    console.log(`[worker] Found ${users.length} user(s) to process`);

    for (const user of users) {
      try {
        await processUser(user, tmpDir);
        console.log(`[worker] ✓ Done: ${user.email}`);
      } catch (err) {
        console.error(`[worker] ✗ Error for user ${user.email}:`, err.message);
        if (err.stack) console.error(err.stack);
      }
    }
  } finally {
    await cleanup();
  }

  console.log('=== YTworker finished at', new Date().toISOString(), '===');
}

runWorker().catch((err) => {
  console.error('[fatal] Worker crashed:', err.message, err.stack);
  process.exit(1);
});
