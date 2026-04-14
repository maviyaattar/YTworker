'use strict';

const axios = require('axios');
const fs = require('fs-extra');
const path = require('path');
const os = require('os');
const { spawn } = require('child_process');
const { google } = require('googleapis');
const cloudinary = require('cloudinary').v2;

// ─── Cloudinary Config ──────────────────────────────────────────────────────

cloudinary.config({
  cloud_name: process.env.CLOUDINARY_CLOUD_NAME,
  api_key:    process.env.CLOUDINARY_API_KEY,
  api_secret: process.env.CLOUDINARY_API_SECRET,
});

// ─── Helpers ─────────────────────────────────────────────────────────────────

/**
 * Spawn a child process and resolve/reject based on exit code.
 * stdout/stderr are forwarded to the parent process for visibility in Actions.
 */
function run(cmd, args) {
  return new Promise((resolve, reject) => {
    console.log(`  [run] ${cmd} ${args.join(' ')}`);
    const p = spawn(cmd, args, { stdio: 'inherit' });
    p.on('error', (err) => reject(new Error(`Failed to start '${cmd}': ${err.message}`)));
    p.on('close', (code) => {
      if (code === 0) resolve();
      else reject(new Error(`'${cmd} ${args.join(' ')}' exited with code ${code}`));
    });
  });
}

/**
 * Download a URL to a local file path using axios streaming.
 */
async function downloadFile(url, destPath) {
  const writer = fs.createWriteStream(destPath);
  const response = await axios.get(url, { responseType: 'stream' });
  await new Promise((resolve, reject) => {
    response.data.pipe(writer);
    writer.on('finish', resolve);
    writer.on('error', reject);
  });
}

// ─── Quote Generation ────────────────────────────────────────────────────────

const PROMPT_MAP = {
  islamic:    'Write a short Islamic reminder (2–3 sentences, no hashtags).',
  motivation: 'Write a motivational quote (1–2 sentences, no hashtags).',
  success:    'Write a success mindset quote (1–2 sentences, no hashtags).',
};

/**
 * Generate a quote for the given content type using Groq Chat Completions.
 */
async function generateQuote(contentType) {
  const prompt = PROMPT_MAP[contentType] || PROMPT_MAP.motivation;

  const res = await axios.post(
    'https://api.groq.com/openai/v1/chat/completions',
    {
      model: 'llama-3.1-8b-instant',
      messages: [{ role: 'user', content: prompt }],
      temperature: 0.8,
      max_tokens: 150,
    },
    {
      headers: {
        Authorization: `Bearer ${process.env.GROQ_API_KEY}`,
        'Content-Type': 'application/json',
      },
      timeout: 30000,
    }
  );

  const quote = res.data.choices[0].message.content.trim();
  return quote;
}

// ─── Background Generation ───────────────────────────────────────────────────

/**
 * Map bgType to a Cloudinary-hosted background image public ID (or URL).
 * These are example placeholder images stored in Cloudinary under a
 * "backgrounds/" folder. Replace public IDs with your actual assets.
 */
const BG_MAP = {
  paper:    'backgrounds/paper',
  dark:     'backgrounds/dark',
  gradient: 'backgrounds/gradient',
  nature:   'backgrounds/nature',
};

/**
 * Fetch (download) the background image for a given bgType to a local file.
 * Returns the local file path.
 */
async function generateBackground(bgType, tmpDir) {
  const publicId = BG_MAP[bgType] || BG_MAP.gradient;
  const bgPath = path.join(tmpDir, 'background.jpg');

  // Build a Cloudinary URL for a 1080×1920 (9:16) crop of the background
  const bgUrl = cloudinary.url(publicId, {
    width:   1080,
    height:  1920,
    crop:    'fill',
    gravity: 'center',
    format:  'jpg',
    quality: 'auto',
  });

  console.log(`  [bg] Downloading background from Cloudinary: ${bgUrl}`);
  await downloadFile(bgUrl, bgPath);
  return bgPath;
}

// ─── ImageMagick Text Overlay ─────────────────────────────────────────────────

/**
 * Render the quote text onto the background image using ImageMagick.
 * Returns the path to the output overlay image.
 *
 * We try 'magick' first (ImageMagick 7) and fall back to 'convert' (IM 6).
 */
async function renderTextOnImage(bgPath, quote, tmpDir) {
  const outputPath = path.join(tmpDir, 'overlay.jpg');

  // Escape the quote for shell safety: replace backslash, double-quote, and
  // any characters that could affect the ImageMagick -annotate argument.
  // We wrap in double-quotes but escape internal double-quotes and backslashes.
  const safeQuote = quote
    .replace(/\\/g, '\\\\')
    .replace(/"/g, '\\"')
    .replace(/\n/g, ' ');

  const args = [
    bgPath,
    '-gravity',    'Center',
    '-fill',       'white',
    '-font',       'DejaVu-Sans-Bold',
    '-pointsize',  '72',
    '-stroke',     'black',
    '-strokewidth','2',
    '-annotate',   '0',
    safeQuote,
    outputPath,
  ];

  // Try ImageMagick 7 ('magick') first, fall back to IM 6 ('convert')
  try {
    await run('magick', args);
  } catch {
    console.log('  [im] magick not found, trying convert (ImageMagick 6)...');
    await run('convert', args);
  }

  return outputPath;
}

// ─── Cloudinary Upload ────────────────────────────────────────────────────────

/**
 * Upload a local file to Cloudinary and return the upload result.
 */
async function uploadToCloudinary(filePath, publicId, resourceType = 'image') {
  return new Promise((resolve, reject) => {
    cloudinary.uploader.upload(
      filePath,
      {
        public_id:     publicId,
        resource_type: resourceType,
        overwrite:     true,
      },
      (err, result) => {
        if (err) reject(err);
        else resolve(result);
      }
    );
  });
}

// ─── Video Generation ─────────────────────────────────────────────────────────

/**
 * Use FFmpeg to produce a 9:16 MP4 short from the overlay image.
 * The image is looped for VIDEO_DURATION_SECONDS with a simple fade-in/out.
 *
 * FFmpeg is pre-installed on GitHub Actions ubuntu-latest runners.
 * Returns the path to the generated local video file.
 */
const VIDEO_DURATION_SECONDS = 10;

async function generateVideo(overlayPath, tmpDir) {
  const videoPath = path.join(tmpDir, 'short.mp4');
  const fadeDuration = 0.5; // seconds for fade in / fade out

  await run('ffmpeg', [
    '-loop',    '1',
    '-i',       overlayPath,
    '-vf',      [
      // Ensure 1080×1920 (9:16), pad if needed
      'scale=1080:1920:force_original_aspect_ratio=decrease',
      'pad=1080:1920:(ow-iw)/2:(oh-ih)/2',
      // Fade in at t=0, fade out near end
      `fade=t=in:st=0:d=${fadeDuration}`,
      `fade=t=out:st=${VIDEO_DURATION_SECONDS - fadeDuration}:d=${fadeDuration}`,
    ].join(','),
    '-c:v',     'libx264',
    '-preset',  'fast',
    '-crf',     '23',
    '-pix_fmt', 'yuv420p',
    '-t',       String(VIDEO_DURATION_SECONDS),
    '-an',                // no audio track
    '-y',                 // overwrite output if it exists
    videoPath,
  ]);

  return videoPath;
}

// ─── YouTube Upload ───────────────────────────────────────────────────────────

/**
 * Build an authenticated OAuth2 client for a user from their stored tokens.
 *
 * youtubeTokens is expected to contain at least:
 *   { access_token, refresh_token, client_id, client_secret, token_type, expiry_date }
 *
 * The worker re-uses the app-level OAuth2 credentials stored in the tokens
 * object itself.  If client_id / client_secret are absent, the call will fail
 * with a descriptive error rather than silently producing bad results.
 */
function buildOAuth2Client(youtubeTokens) {
  const { client_id, client_secret, redirect_uri = 'urn:ietf:wg:oauth:2.0:oob' } = youtubeTokens;

  if (!client_id || !client_secret) {
    throw new Error(
      'youtubeTokens must include client_id and client_secret. ' +
      'Store them alongside access_token / refresh_token in the backend.'
    );
  }

  const oauth2Client = new google.auth.OAuth2(client_id, client_secret, redirect_uri);
  oauth2Client.setCredentials({
    access_token:  youtubeTokens.access_token  || null,
    refresh_token: youtubeTokens.refresh_token || null,
    expiry_date:   youtubeTokens.expiry_date   || null,
    token_type:    youtubeTokens.token_type    || 'Bearer',
  });

  // Automatically refresh the access token before requests if it has expired
  oauth2Client.on('tokens', (tokens) => {
    if (tokens.refresh_token) {
      console.log('  [yt] Received new refresh_token — consider persisting it to backend.');
    }
    console.log('  [yt] Access token refreshed automatically.');
  });

  return oauth2Client;
}

/**
 * Upload the local video file to YouTube as a Short (vertical video).
 * Returns the YouTube video ID on success.
 */
async function uploadToYouTube(videoPath, quote, contentType, youtubeTokens) {
  const auth = buildOAuth2Client(youtubeTokens);

  const youtube = google.youtube({ version: 'v3', auth });

  const titleMap = {
    islamic:    '🕌 Daily Islamic Reminder',
    motivation: '🔥 Daily Motivation',
    success:    '💡 Success Mindset',
  };
  const rawTitle = (titleMap[contentType] || 'Daily Short') + ' #Shorts';
  // Truncate at word boundary to avoid cutting mid-word; YouTube title limit is 100 chars
  const title = rawTitle.length <= 100
    ? rawTitle
    : rawTitle.substring(0, 97).replace(/\s+\S*$/, '') + '...';

  const description =
    `${quote}\n\n` +
    `#Shorts #${contentType || 'motivation'} #YouTubeShorts`;

  const res = await youtube.videos.insert({
    part: ['snippet', 'status'],
    requestBody: {
      snippet: {
        title:       title,
        description: description.substring(0, 5000), // YouTube description limit
        tags:        ['Shorts', contentType, 'YouTubeShorts'],
        categoryId:  '22', // People & Blogs
      },
      status: {
        privacyStatus:           'public',
        selfDeclaredMadeForKids: false,
      },
    },
    media: {
      body: fs.createReadStream(videoPath),
    },
  });

  return res.data.id;
}

// ─── Per-User Pipeline ────────────────────────────────────────────────────────

/**
 * Run the full pipeline for a single user.
 * The temp directory is cleaned up regardless of success or failure.
 */
async function processUser(user) {
  const jobId  = `${user._id}_${Date.now()}`;
  const tmpDir = path.join(os.tmpdir(), 'ytworker', jobId);

  await fs.ensureDir(tmpDir);
  console.log(`\n[user:${user.email}] Starting pipeline (tmpDir: ${tmpDir})`);

  try {
    // 1. Generate quote
    console.log(`[user:${user.email}] Step 1 — Generating quote (contentType: ${user.contentType})`);
    const quote = await generateQuote(user.contentType);
    console.log(`[user:${user.email}] Quote: "${quote}"`);

    // 2. Generate background
    console.log(`[user:${user.email}] Step 2 — Fetching background (bgType: ${user.bgType})`);
    const bgPath = await generateBackground(user.bgType, tmpDir);

    // 3. Render text on image
    console.log(`[user:${user.email}] Step 3 — Rendering text overlay with ImageMagick`);
    const overlayPath = await renderTextOnImage(bgPath, quote, tmpDir);

    // 4. Upload overlay to Cloudinary
    const overlayPublicId = `ytshorts/${user._id}_overlay_${Date.now()}`;
    console.log(`[user:${user.email}] Step 4 — Uploading overlay to Cloudinary (${overlayPublicId})`);
    await uploadToCloudinary(overlayPath, overlayPublicId, 'image');

    // 5. Generate final video locally with FFmpeg
    console.log(`[user:${user.email}] Step 5 — Generating final video with FFmpeg`);
    const videoPath = await generateVideo(overlayPath, tmpDir);

    // 6. Upload to YouTube
    console.log(`[user:${user.email}] Step 6 — Uploading to YouTube`);
    const ytVideoId = await uploadToYouTube(videoPath, quote, user.contentType, user.youtubeTokens);
    console.log(`[user:${user.email}] ✅ Done! YouTube video ID: ${ytVideoId}`);

  } finally {
    // Always clean up temp files for this user
    await fs.remove(tmpDir);
    console.log(`[user:${user.email}] Temp dir cleaned up.`);
  }
}

// ─── Main Worker ──────────────────────────────────────────────────────────────

async function runWorker() {
  console.log('=== YTworker starting ===');
  console.log(`Timestamp: ${new Date().toISOString()}`);

  // Validate required environment variables before doing any work
  const REQUIRED_ENV = [
    'GROQ_API_KEY',
    'CLOUDINARY_CLOUD_NAME',
    'CLOUDINARY_API_KEY',
    'CLOUDINARY_API_SECRET',
    'BACKEND_URL',
    'WORKER_SECRET',
  ];

  const missing = REQUIRED_ENV.filter((k) => !process.env[k]);
  if (missing.length > 0) {
    throw new Error(`Missing required environment variables: ${missing.join(', ')}`);
  }

  // Fetch users from backend
  console.log(`\nFetching users from ${process.env.BACKEND_URL}/worker/users ...`);
  const res = await axios.get(`${process.env.BACKEND_URL}/worker/users`, {
    headers: { Authorization: `Bearer ${process.env.WORKER_SECRET}` },
    timeout: 15000,
  });

  const users = res.data.users;
  if (!Array.isArray(users) || users.length === 0) {
    console.log('No users returned from backend. Nothing to do.');
    return;
  }

  console.log(`Found ${users.length} user(s) to process.\n`);

  let successCount = 0;
  let failCount    = 0;

  for (const user of users) {
    try {
      await processUser(user);
      successCount++;
    } catch (err) {
      failCount++;
      console.error(`[user:${user.email}] ❌ Pipeline failed: ${err.message}`);
      if (err.response) {
        // Axios error — log response details
        console.error(
          `  HTTP ${err.response.status}: ${JSON.stringify(err.response.data)}`
        );
      }
    }
  }

  console.log(`\n=== YTworker finished ===`);
  console.log(`Results: ${successCount} succeeded, ${failCount} failed out of ${users.length} users.`);
}

// ─── Cleanup on exit ─────────────────────────────────────────────────────────

const WORKER_TMP_ROOT = path.join(os.tmpdir(), 'ytworker');

async function cleanup() {
  try {
    await fs.remove(WORKER_TMP_ROOT);
  } catch {
    // Best-effort cleanup; ignore errors
  }
}

process.on('exit', () => {
  // Synchronous best-effort cleanup on normal exit
  try {
    fs.removeSync(WORKER_TMP_ROOT);
  } catch {
    // Ignore
  }
});

// ─── Entry point ─────────────────────────────────────────────────────────────

runWorker()
  .catch(async (err) => {
    console.error('Fatal error in runWorker:', err.message);
    await cleanup();
    process.exit(1);
  });
