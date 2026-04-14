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
// Helper – download a URL to a local file path
// ---------------------------------------------------------------------------
async function downloadFile(url, dest) {
  const response = await axios.get(url, { responseType: 'stream' });
  const writer = fs.createWriteStream(dest);
  await new Promise((resolve, reject) => {
    response.data.pipe(writer);
    writer.on('finish', resolve);
    writer.on('error', reject);
  });
}

// ---------------------------------------------------------------------------
// 1. Generate quote via Groq Chat Completions
// ---------------------------------------------------------------------------
const PROMPT_MAP = {
  islamic: 'Write a short Islamic reminder (1-2 sentences). Output only the reminder text.',
  motivation: 'Write a short motivational quote (1-2 sentences). Output only the quote.',
  success: 'Write a short success-mindset quote (1-2 sentences). Output only the quote.',
};

async function generateQuote(contentType) {
  const prompt = PROMPT_MAP[contentType] || PROMPT_MAP.motivation;
  const res = await axios.post(
    'https://api.groq.com/openai/v1/chat/completions',
    {
      model: 'llama-3.1-8b-instant',
      messages: [{ role: 'user', content: prompt }],
      max_tokens: 120,
      temperature: 0.8,
    },
    {
      headers: {
        Authorization: `Bearer ${process.env.GROQ_API_KEY}`,
        'Content-Type': 'application/json',
      },
      timeout: 30000,
    },
  );
  return res.data.choices[0].message.content.trim();
}

// ---------------------------------------------------------------------------
// 2. Generate background image
//    Uses a Cloudinary placeholder URL keyed on bgType so the worker does not
//    need a local image file.  You can swap this for any image generation API.
// ---------------------------------------------------------------------------
const BG_COLOR_MAP = {
  paper: 'f5f0e8',
  dark: '1a1a2e',
  nature: '2d6a4f',
  ocean: '023e8a',
  sunset: 'ff6b35',
};

async function generateBackground(bgType, tmpDir) {
  const color = BG_COLOR_MAP[bgType] || BG_COLOR_MAP.dark;
  const bgPath = path.join(tmpDir, 'background.png');

  // Create a solid-color 1080×1920 background with ImageMagick
  await run('convert', [
    '-size', '1080x1920',
    `xc:#${color}`,
    bgPath,
  ]);

  return bgPath;
}

// ---------------------------------------------------------------------------
// 3. Render quote text onto background using ImageMagick
// ---------------------------------------------------------------------------
async function renderTextOnImage(bgPath, quote, tmpDir) {
  const outputPath = path.join(tmpDir, 'overlay.png');

  // Sanitise the quote so it is safe to pass as an ImageMagick label
  const safeQuote = quote.replace(/'/g, '\u2019').replace(/\n/g, ' ');

  await run('convert', [
    bgPath,
    '-gravity', 'Center',
    '-fill', 'white',
    '-pointsize', '72',
    '-font', 'DejaVu-Sans-Bold',
    '-annotate', '0',
    safeQuote,
    outputPath,
  ]);

  return outputPath;
}

// ---------------------------------------------------------------------------
// 4. Upload overlay image to Cloudinary
// ---------------------------------------------------------------------------
async function uploadToCloudinary(filePath, publicId) {
  return new Promise((resolve, reject) => {
    cloudinary.uploader.upload(
      filePath,
      { public_id: publicId, resource_type: 'image', overwrite: true },
      (err, result) => {
        if (err) reject(err);
        else resolve(result);
      },
    );
  });
}

// ---------------------------------------------------------------------------
// 5. Generate final 9:16 short video using Cloudinary video transformations
// ---------------------------------------------------------------------------
async function generateShortVideo(overlayPublicId) {
  // Build a video URL using Cloudinary transformations:
  // - Use a base video or create one from an image with duration
  // - Overlay the text image
  // - Crop to 9:16 (1080×1920)
  // We create a 10-second video from the overlay image via Cloudinary's
  // image-to-video feature (dl_video) combined with the video transformation.
  const videoUrl = cloudinary.url(overlayPublicId, {
    resource_type: 'video',
    transformation: [
      { width: 1080, height: 1920, crop: 'fill', gravity: 'center' },
      { duration: '10', effect: 'loop' },
      { quality: 'auto', fetch_format: 'mp4' },
    ],
    format: 'mp4',
  });

  return videoUrl;
}

// ---------------------------------------------------------------------------
// 6. Download the generated video from Cloudinary
// ---------------------------------------------------------------------------
async function downloadVideo(videoUrl, tmpDir) {
  const videoPath = path.join(tmpDir, 'short.mp4');
  console.log('  [video] Downloading from Cloudinary…');
  await downloadFile(videoUrl, videoPath);
  return videoPath;
}

// ---------------------------------------------------------------------------
// 7. Upload video to YouTube using googleapis
// ---------------------------------------------------------------------------
async function uploadToYouTube(videoPath, title, youtubeTokens) {
  const { clientId, clientSecret, refreshToken, accessToken } = youtubeTokens;

  const oauth2Client = new google.auth.OAuth2(clientId, clientSecret);

  if (refreshToken) {
    oauth2Client.setCredentials({ refresh_token: refreshToken });
    // Force refresh to get a fresh access token
    const tokenResponse = await oauth2Client.refreshAccessToken();
    oauth2Client.setCredentials(tokenResponse.credentials);
  } else if (accessToken) {
    oauth2Client.setCredentials({ access_token: accessToken });
  } else {
    throw new Error('No valid YouTube credentials (need refreshToken or accessToken)');
  }

  const youtube = google.youtube({ version: 'v3', auth: oauth2Client });

  const res = await youtube.videos.insert({
    part: ['snippet', 'status'],
    requestBody: {
      snippet: {
        title,
        description: 'Auto-generated YouTube Short',
        tags: ['Shorts', 'AI', 'motivation'],
        categoryId: '22',
        defaultLanguage: 'en',
      },
      status: {
        privacyStatus: 'public',
        selfDeclaredMadeForKids: false,
        madeForKids: false,
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

    // 1. Generate quote
    console.log('  [1/7] Generating quote…');
    const quote = await generateQuote(contentType);
    console.log(`  [quote] "${quote}"`);

    // 2. Generate background
    console.log('  [2/7] Generating background…');
    const bgPath = await generateBackground(bgType, userTmpDir);

    // 3. Render text on image
    console.log('  [3/7] Rendering text on image…');
    const overlayPath = await renderTextOnImage(bgPath, quote, userTmpDir);

    // 4. Upload overlay to Cloudinary
    const overlayPublicId = `ytworker/overlay_${jobId}`;
    console.log(`  [4/7] Uploading overlay to Cloudinary (${overlayPublicId})…`);
    await uploadToCloudinary(overlayPath, overlayPublicId);

    // 5. Generate short video URL via Cloudinary transformations
    console.log('  [5/7] Generating video via Cloudinary…');
    const videoUrl = await generateShortVideo(overlayPublicId);
    console.log(`  [video url] ${videoUrl}`);

    // 6. Download video
    console.log('  [6/7] Downloading video…');
    const videoPath = await downloadVideo(videoUrl, userTmpDir);

    // 7. Upload to YouTube
    console.log('  [7/7] Uploading to YouTube…');
    const videoTitle = `${quote.slice(0, 80)} #Shorts`;
    const ytResult = await uploadToYouTube(videoPath, videoTitle, youtubeTokens);
    console.log(`  [youtube] Uploaded! Video ID: ${ytResult.id}`);
  } finally {
    // Clean up temp files for this user
    await fs.remove(userTmpDir).catch(() => {});
    console.log(`  [cleanup] Removed temp dir for ${email}`);
  }
}

// ---------------------------------------------------------------------------
// Main worker entry point
// ---------------------------------------------------------------------------
async function runWorker() {
  console.log('=== YTworker started at', new Date().toISOString(), '===');

  // Validate required env vars
  const required = [
    'GROQ_API_KEY',
    'CLOUDINARY_CLOUD_NAME',
    'CLOUDINARY_API_KEY',
    'CLOUDINARY_API_SECRET',
    'BACKEND_URL',
    'WORKER_SECRET',
  ];
  const missing = required.filter((k) => !process.env[k]);
  if (missing.length > 0) {
    console.error('[fatal] Missing required environment variables:', missing.join(', '));
    process.exit(1);
  }

  // Shared temp directory for this run
  const tmpDir = path.join(os.tmpdir(), `ytworker_${Date.now()}`);
  await fs.ensureDir(tmpDir);

  // Register cleanup on exit
  const cleanup = async () => {
    await fs.remove(tmpDir).catch(() => {});
    console.log('[cleanup] Removed shared temp dir');
  };
  process.on('exit', () => fs.removeSync(tmpDir));
  process.on('SIGINT', async () => { await cleanup(); process.exit(0); });
  process.on('SIGTERM', async () => { await cleanup(); process.exit(0); });

  try {
    // Fetch users from backend
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

    // Process each user sequentially; errors are caught per-user
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
