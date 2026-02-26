/**
 * Benchmark GIF visualizer.
 *
 * Reads benchmark JSON results and generates an animated GIF showing:
 *   1. Throughput comparison (jobs/s) - hero metric
 *   2. Latency percentiles (p50/p90/p99) - reliability metric
 *   3. Memory footprint (RSS delta) - efficiency metric
 *
 * Usage:
 *   npx tsx benchmarks/visualize.ts <results.json> [output.gif]
 *   npx tsx benchmarks/visualize.ts --sample [output.gif]
 */

import * as fs from 'node:fs';
import * as path from 'node:path';
import { createCanvas, type SKRSContext2D } from '@napi-rs/canvas';
import GIFEncoder from 'gif-encoder-2';
import type { BenchmarkResults } from './run';

// --- Layout ---

const WIDTH = 840;
const HEIGHT = 480;
const FPS = 20;
const BAR_AREA_LEFT = 220;
const BAR_AREA_RIGHT = WIDTH - 160; // reserve space for value labels
const BAR_MAX_WIDTH = BAR_AREA_RIGHT - BAR_AREA_LEFT;

// Timing (frames)
const SCENE_ANIMATE = 28;  // 1.4s bar growth
const SCENE_HOLD = 36;     // 1.8s hold
const SCENE_TOTAL = SCENE_ANIMATE + SCENE_HOLD;
const TRANSITION_FRAMES = 6; // 0.3s fade between scenes

// Colors (GitHub dark theme inspired)
const BG = '#0d1117';
const GLIDE_COLOR = '#3fb950';
const BULL_COLOR = '#6e7681';
const TEXT_COLOR = '#e6edf3';
const DIM_TEXT = '#8b949e';
const MULTIPLIER_COLOR = '#f0883e';
const DIVIDER_COLOR = '#21262d';
const BADGE_BG = '#1c2128';

// --- Sample data for --sample mode ---

const SAMPLE_RESULTS: BenchmarkResults = {
  meta: { date: new Date().toISOString(), node: process.version, platform: `${process.platform} ${process.arch}` },
  throughput: {
    add: {
      glideMq: { library: 'glide-mq', count: 11500, elapsed: 5000, rate: 2300 },
      bullMq: { library: 'bullmq', count: 5000, elapsed: 5000, rate: 1000 },
    },
    process: [
      { library: 'glide-mq', target: 2000, completed: 2000, concurrency: 10, elapsed: 1250, rate: 1600 },
      { library: 'bullmq', target: 2000, completed: 2000, concurrency: 10, elapsed: 2857, rate: 700 },
    ],
  },
  latency: {
    serial: {
      glideMq: { p50: 1.8, p90: 2.5, p99: 4.1 },
      bullMq: { p50: 3.2, p90: 5.8, p99: 9.7 },
    },
    pipelined: {
      glideMq: { addToStart: { p50: 2.1, p90: 3.8, p99: 6.2 }, addToComplete: { p50: 3.5, p90: 5.2, p99: 8.1 } },
      bullMq: { addToStart: { p50: 4.8, p90: 8.5, p99: 14.3 }, addToComplete: { p50: 7.2, p90: 12.1, p99: 19.8 } },
    },
  },
  memory: {
    glideMq: { rssDelta: 18.5, heapDelta: 12.3, rssBefore: 85, rssAfter: 103.5, heapBefore: 42, heapAfter: 54.3 },
    bullMq: { rssDelta: 47.2, heapDelta: 31.8, rssBefore: 92, rssAfter: 139.2, heapBefore: 48, heapAfter: 79.8 },
  },
};

// --- Drawing primitives ---

function roundRect(ctx: SKRSContext2D, x: number, y: number, w: number, h: number, r: number): void {
  if (w <= 0) return;
  const radius = Math.min(r, w / 2, h / 2);
  ctx.beginPath();
  ctx.moveTo(x + radius, y);
  ctx.lineTo(x + w - radius, y);
  ctx.quadraticCurveTo(x + w, y, x + w, y + radius);
  ctx.lineTo(x + w, y + h - radius);
  ctx.quadraticCurveTo(x + w, y + h, x + w - radius, y + h);
  ctx.lineTo(x + radius, y + h);
  ctx.quadraticCurveTo(x, y + h, x, y + h - radius);
  ctx.lineTo(x, y + radius);
  ctx.quadraticCurveTo(x, y, x + radius, y);
  ctx.closePath();
  ctx.fill();
}

function easeOutCubic(t: number): number {
  return 1 - Math.pow(1 - t, 3);
}

function fmtVal(n: number): string {
  if (n >= 1000) return n.toLocaleString('en-US', { maximumFractionDigits: 0 });
  if (n >= 100) return n.toFixed(0);
  if (n >= 10) return n.toFixed(1);
  return n.toFixed(2);
}

// --- Shared drawing functions ---

function drawBackground(ctx: SKRSContext2D): void {
  ctx.fillStyle = BG;
  ctx.fillRect(0, 0, WIDTH, HEIGHT);
}

function drawHeader(ctx: SKRSContext2D, title: string, subtitle: string): void {
  ctx.fillStyle = TEXT_COLOR;
  ctx.font = 'bold 28px sans-serif';
  ctx.textAlign = 'left';
  ctx.fillText(title, 40, 50);

  ctx.fillStyle = DIM_TEXT;
  ctx.font = '14px sans-serif';
  ctx.fillText(subtitle, 40, 74);

  ctx.strokeStyle = DIVIDER_COLOR;
  ctx.lineWidth = 1;
  ctx.beginPath();
  ctx.moveTo(40, 90);
  ctx.lineTo(WIDTH - 40, 90);
  ctx.stroke();
}

function drawLegend(ctx: SKRSContext2D, y: number): void {
  ctx.fillStyle = GLIDE_COLOR;
  roundRect(ctx, 40, y, 14, 14, 3);
  ctx.fillStyle = TEXT_COLOR;
  ctx.font = 'bold 13px sans-serif';
  ctx.textAlign = 'left';
  ctx.fillText('glide-mq', 60, y + 12);

  ctx.fillStyle = BULL_COLOR;
  roundRect(ctx, 150, y, 14, 14, 3);
  ctx.fillStyle = DIM_TEXT;
  ctx.font = '13px sans-serif';
  ctx.fillText('BullMQ', 170, y + 12);
}

function drawSceneIndicator(ctx: SKRSContext2D, current: number, total: number): void {
  const dotSize = 6;
  const dotGap = 14;
  const totalW = total * dotSize + (total - 1) * (dotGap - dotSize);
  const startX = (WIDTH - totalW) / 2;
  const y = HEIGHT - 30;

  for (let i = 0; i < total; i++) {
    ctx.fillStyle = i === current ? GLIDE_COLOR : DIVIDER_COLOR;
    ctx.beginPath();
    ctx.arc(startX + i * dotGap + dotSize / 2, y, dotSize / 2, 0, Math.PI * 2);
    ctx.fill();
  }
}

function drawFooter(ctx: SKRSContext2D, sceneIdx: number, totalScenes: number): void {
  ctx.fillStyle = DIM_TEXT;
  ctx.font = '11px sans-serif';
  ctx.textAlign = 'center';
  ctx.fillText('github.com/avifenesh/glide-mq', WIDTH / 2, HEIGHT - 10);
  drawSceneIndicator(ctx, sceneIdx, totalScenes);
}

interface BarPair {
  label: string;
  glideVal: number;
  bullVal: number;
  unit: string;
}

function drawBarPairs(
  ctx: SKRSContext2D,
  pairs: BarPair[],
  progress: number,
  startY: number,
  barHeight: number,
  gap: number,
): void {
  const maxVal = Math.max(...pairs.flatMap((p) => [p.glideVal, p.bullVal]));
  if (maxVal === 0) return;
  const eased = easeOutCubic(Math.min(1, progress));

  for (let i = 0; i < pairs.length; i++) {
    const pair = pairs[i];
    const blockH = barHeight * 2 + 4;
    const y = startY + i * (blockH + gap);

    // Row label
    ctx.fillStyle = TEXT_COLOR;
    ctx.font = 'bold 13px sans-serif';
    ctx.textAlign = 'right';
    ctx.fillText(pair.label, BAR_AREA_LEFT - 14, y + blockH / 2 + 4);

    // glide-mq bar
    const glideW = Math.max(2, (pair.glideVal / maxVal) * BAR_MAX_WIDTH * eased);
    ctx.fillStyle = GLIDE_COLOR;
    roundRect(ctx, BAR_AREA_LEFT, y, glideW, barHeight, 4);

    // BullMQ bar
    const bullW = Math.max(2, (pair.bullVal / maxVal) * BAR_MAX_WIDTH * eased);
    ctx.fillStyle = BULL_COLOR;
    roundRect(ctx, BAR_AREA_LEFT, y + barHeight + 4, bullW, barHeight, 4);

    // Value labels - always to the right of bars
    if (progress > 0.3) {
      const valAlpha = Math.min(1, (progress - 0.3) / 0.25);
      const gVal = pair.glideVal * eased;
      const bVal = pair.bullVal * eased;

      ctx.globalAlpha = valAlpha;
      ctx.font = 'bold 12px sans-serif';
      ctx.textAlign = 'left';

      ctx.fillStyle = GLIDE_COLOR;
      ctx.fillText(`${fmtVal(gVal)} ${pair.unit}`, BAR_AREA_LEFT + glideW + 8, y + barHeight - 2);
      ctx.fillStyle = DIM_TEXT;
      ctx.fillText(`${fmtVal(bVal)} ${pair.unit}`, BAR_AREA_LEFT + bullW + 8, y + barHeight * 2 + 2);

      ctx.globalAlpha = 1;
    }
  }
}

function drawBadge(ctx: SKRSContext2D, text: string, progress: number): void {
  if (progress < 0.65) return;
  const alpha = Math.min(1, (progress - 0.65) / 0.2);
  ctx.globalAlpha = alpha;

  // Measure text to draw background pill
  ctx.font = 'bold 20px sans-serif';
  const metrics = ctx.measureText(text);
  const padX = 16;
  const padY = 8;
  const w = metrics.width + padX * 2;
  const h = 32 + padY;
  const x = WIDTH - 50 - w;
  const y = HEIGHT - 75 - h / 2;

  // Background pill
  ctx.fillStyle = BADGE_BG;
  roundRect(ctx, x, y, w, h, 8);

  // Border
  ctx.strokeStyle = MULTIPLIER_COLOR;
  ctx.lineWidth = 1.5;
  ctx.beginPath();
  ctx.moveTo(x + 8, y);
  ctx.lineTo(x + w - 8, y);
  ctx.quadraticCurveTo(x + w, y, x + w, y + 8);
  ctx.lineTo(x + w, y + h - 8);
  ctx.quadraticCurveTo(x + w, y + h, x + w - 8, y + h);
  ctx.lineTo(x + 8, y + h);
  ctx.quadraticCurveTo(x, y + h, x, y + h - 8);
  ctx.lineTo(x, y + 8);
  ctx.quadraticCurveTo(x, y, x + 8, y);
  ctx.closePath();
  ctx.stroke();

  // Text
  ctx.fillStyle = MULTIPLIER_COLOR;
  ctx.textAlign = 'center';
  ctx.fillText(text, x + w / 2, y + h / 2 + 7);

  ctx.globalAlpha = 1;
}

// --- Scene renderers ---

function renderThroughputFrame(ctx: SKRSContext2D, data: BenchmarkResults, frame: number, sceneIdx: number, totalScenes: number): void {
  drawBackground(ctx);
  drawHeader(ctx, 'Throughput', 'Jobs per second  |  higher is better');
  drawLegend(ctx, 102);

  const progress = Math.min(1, frame / SCENE_ANIMATE);

  const addRate = data.throughput!.add;
  const glideProc = data.throughput!.process.find((r) => r.library === 'glide-mq' && r.concurrency === 10 && r.target === 2000)
    || data.throughput!.process.find((r) => r.library === 'glide-mq' && r.concurrency === 10)
    || data.throughput!.process.find((r) => r.library === 'glide-mq');
  const bullProc = data.throughput!.process.find((r) => r.library === 'bullmq' && r.concurrency === 10 && r.target === 2000)
    || data.throughput!.process.find((r) => r.library === 'bullmq' && r.concurrency === 10)
    || data.throughput!.process.find((r) => r.library === 'bullmq');

  const pairs: BarPair[] = [
    { label: 'Add Rate', glideVal: addRate.glideMq.rate, bullVal: addRate.bullMq.rate, unit: 'jobs/s' },
  ];
  if (glideProc && bullProc) {
    pairs.push({ label: 'Process (c=10)', glideVal: glideProc.rate, bullVal: bullProc.rate, unit: 'jobs/s' });
  }

  drawBarPairs(ctx, pairs, progress, 140, 30, 36);

  const multiplier = (addRate.glideMq.rate / addRate.bullMq.rate).toFixed(1);
  drawBadge(ctx, `${multiplier}x faster throughput`, progress);
  drawFooter(ctx, sceneIdx, totalScenes);
}

function renderLatencyFrame(ctx: SKRSContext2D, data: BenchmarkResults, frame: number, sceneIdx: number, totalScenes: number): void {
  drawBackground(ctx);
  drawHeader(ctx, 'Latency', 'Round-trip time in ms  |  lower is better');
  drawLegend(ctx, 102);

  const progress = Math.min(1, frame / SCENE_ANIMATE);
  const serial = data.latency!.serial;

  const pairs: BarPair[] = [
    { label: 'p50 (median)', glideVal: serial.glideMq.p50, bullVal: serial.bullMq.p50, unit: 'ms' },
    { label: 'p90', glideVal: serial.glideMq.p90, bullVal: serial.bullMq.p90, unit: 'ms' },
    { label: 'p99 (tail)', glideVal: serial.glideMq.p99, bullVal: serial.bullMq.p99, unit: 'ms' },
  ];

  drawBarPairs(ctx, pairs, progress, 132, 24, 22);

  const p99Pct = (((serial.bullMq.p99 - serial.glideMq.p99) / serial.bullMq.p99) * 100).toFixed(0);
  drawBadge(ctx, `${p99Pct}% lower p99 latency`, progress);
  drawFooter(ctx, sceneIdx, totalScenes);
}

function renderMemoryFrame(ctx: SKRSContext2D, data: BenchmarkResults, frame: number, sceneIdx: number, totalScenes: number): void {
  drawBackground(ctx);
  drawHeader(ctx, 'Memory', 'RSS delta after 10k jobs  |  lower is better');
  drawLegend(ctx, 102);

  const progress = Math.min(1, frame / SCENE_ANIMATE);
  const mem = data.memory!;

  const pairs: BarPair[] = [
    { label: 'RSS Delta', glideVal: mem.glideMq.rssDelta, bullVal: mem.bullMq.rssDelta, unit: 'MB' },
    { label: 'Heap Delta', glideVal: mem.glideMq.heapDelta, bullVal: mem.bullMq.heapDelta, unit: 'MB' },
  ];

  drawBarPairs(ctx, pairs, progress, 150, 34, 44);

  const pct = (((mem.bullMq.rssDelta - mem.glideMq.rssDelta) / mem.bullMq.rssDelta) * 100).toFixed(0);
  drawBadge(ctx, `${pct}% less memory`, progress);
  drawFooter(ctx, sceneIdx, totalScenes);
}

// --- GIF generation ---

type SceneRenderer = (ctx: SKRSContext2D, data: BenchmarkResults, frame: number, sceneIdx: number, totalScenes: number) => void;

function generateGif(data: BenchmarkResults, outputPath: string): void {
  const canvas = createCanvas(WIDTH, HEIGHT);
  const ctx = canvas.getContext('2d');

  const encoder = new GIFEncoder(WIDTH, HEIGHT);
  encoder.setDelay(Math.round(1000 / FPS));
  encoder.setRepeat(0);
  encoder.setQuality(10);
  encoder.start();

  const scenes: SceneRenderer[] = [];
  if (data.throughput) scenes.push(renderThroughputFrame);
  if (data.latency) scenes.push(renderLatencyFrame);
  if (data.memory) scenes.push(renderMemoryFrame);

  if (scenes.length === 0) {
    console.error('No benchmark data to visualize. Run benchmarks with --json first.');
    process.exit(1);
  }

  for (let s = 0; s < scenes.length; s++) {
    // Scene frames
    for (let f = 0; f < SCENE_TOTAL; f++) {
      scenes[s](ctx, data, f, s, scenes.length);
      encoder.addFrame(ctx as any);
    }

    // Transition fade to black between scenes
    if (s < scenes.length - 1) {
      for (let t = 0; t < TRANSITION_FRAMES; t++) {
        const fadeProgress = t / TRANSITION_FRAMES;

        // Render the last frame of current scene
        scenes[s](ctx, data, SCENE_TOTAL - 1, s, scenes.length);

        // Overlay fade
        ctx.globalAlpha = fadeProgress * 0.85;
        ctx.fillStyle = BG;
        ctx.fillRect(0, 0, WIDTH, HEIGHT);
        ctx.globalAlpha = 1;

        encoder.addFrame(ctx as any);
      }
    }
  }

  encoder.finish();
  const buf = encoder.out.getData();
  fs.writeFileSync(outputPath, buf);

  const sizeMB = (buf.length / 1024 / 1024).toFixed(2);
  const totalFrames = scenes.length * SCENE_TOTAL + (scenes.length - 1) * TRANSITION_FRAMES;
  const durationSec = (totalFrames / FPS).toFixed(1);
  console.log(`GIF written to ${outputPath} (${sizeMB} MB, ${totalFrames} frames, ${durationSec}s)`);
}

// --- CLI ---

function main(): void {
  const args = process.argv.slice(2);

  if (args.length === 0 || args.includes('--help') || args.includes('-h')) {
    console.log('Usage:');
    console.log('  npx tsx benchmarks/visualize.ts <results.json> [output.gif]');
    console.log('  npx tsx benchmarks/visualize.ts --sample [output.gif]');
    console.log('');
    console.log('Options:');
    console.log('  --sample   Use sample data (no Valkey needed)');
    console.log('  --help     Show this help');
    process.exit(0);
  }

  const useSample = args.includes('--sample');
  const nonFlagArgs = args.filter((a) => !a.startsWith('--'));
  const defaultOutput = path.join(process.cwd(), 'benchmarks', 'benchmark.gif');

  let data: BenchmarkResults;
  let outputPath: string;

  if (useSample) {
    data = SAMPLE_RESULTS;
    outputPath = nonFlagArgs[0] || defaultOutput;
    console.log('Using sample benchmark data.');
  } else {
    const inputPath = nonFlagArgs[0];
    if (!inputPath) {
      console.error('Provide a results JSON path or use --sample');
      process.exit(1);
    }
    if (!fs.existsSync(inputPath)) {
      console.error(`File not found: ${inputPath}`);
      process.exit(1);
    }
    data = JSON.parse(fs.readFileSync(inputPath, 'utf-8'));
    outputPath = nonFlagArgs[1] || defaultOutput;
  }

  generateGif(data, outputPath);
}

main();
