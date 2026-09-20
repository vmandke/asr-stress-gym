// Verify that the framing code in the mic page actually speaks this
// gateway's protocol, by running THAT code — extracted from the page, not
// reimplemented here — against a live gateway with real corpus audio.
//
// A reimplementation would test the wrong thing. The failure mode worth
// catching is a page whose header is subtly wrong (wrong endianness, a
// num_samples that disagrees with the payload, a control frame the gateway
// rejects), and a second copy of the encoder in this file could be correct
// while the page stayed broken.
//
//   node scripts/test_mic_protocol.mjs [ws://localhost:7070/ws]
//
// Exits non-zero with a reason if the gateway does not produce a final.

import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const here = dirname(fileURLToPath(import.meta.url));
const root = join(here, "..");
const WS = process.argv[2] || "ws://localhost:7070/ws";
const CLIP = join(root, "corpus", "02_number_transfer.wav");

// --- lift the page's own encoder out of the page ----------------------
const page = readFileSync(join(root, "internal/dash/static/mic.html"), "utf8");
const slice = page.match(/const SAMPLE_RATE[\s\S]*?(?=\/\/ ---- end protocol block)/);
if (!slice) {
  console.error("FAIL: could not find the protocol block in mic.html — did it move?");
  process.exit(1);
}
const { SAMPLE_RATE, FRAME_SAMPLES, encodeAudio, encodeControl } =
  await import("data:text/javascript," + encodeURIComponent(
    slice[0] + "\nexport { SAMPLE_RATE, FRAME_SAMPLES, encodeAudio, encodeControl };"
  ));

// --- the clip, as float32 mono 16k ------------------------------------
function readWav(path) {
  const b = readFileSync(path);
  if (b.toString("ascii", 0, 4) !== "RIFF") throw new Error("not a WAV");
  let off = 12, rate = 0, bits = 0, ch = 0, data = null;
  while (off + 8 <= b.length) {
    const id = b.toString("ascii", off, off + 4);
    const size = b.readUInt32LE(off + 4);
    if (id === "fmt ") {
      ch = b.readUInt16LE(off + 10);
      rate = b.readUInt32LE(off + 12);
      bits = b.readUInt16LE(off + 22);
    } else if (id === "data") {
      data = b.subarray(off + 8, off + 8 + size);
    }
    off += 8 + size + (size % 2);
  }
  if (rate !== 16000 || ch !== 1 || bits !== 16) {
    throw new Error(`expected mono 16k s16le, got ${rate}Hz ${ch}ch ${bits}bit`);
  }
  const out = new Float32Array(data.length / 2);
  for (let i = 0; i < out.length; i++) out[i] = data.readInt16LE(i * 2) / 32768;
  return out;
}

const pcm = readWav(CLIP);
const frames = [];
for (let i = 0; i + FRAME_SAMPLES <= pcm.length; i += FRAME_SAMPLES) {
  frames.push(pcm.subarray(i, i + FRAME_SAMPLES));
}

console.log(`clip    : ${CLIP.replace(root + "/", "")}  ${(pcm.length / SAMPLE_RATE).toFixed(2)}s`);
console.log(`frames  : ${frames.length} x ${FRAME_SAMPLES} samples (${FRAME_SAMPLES / 16} ms)`);
console.log(`gateway : ${WS}\n`);

// --- stream it --------------------------------------------------------
const ws = new WebSocket(WS);
ws.binaryType = "arraybuffer";

let seq = 0, partials = 0, finalText = null, sessionId = null, errored = null;
let opened = false, lastPartial = "";
const t0 = Date.now();

const fail = (why) => { console.error("\nFAIL: " + why); process.exit(1); };
setTimeout(() => fail("timed out with no final"), 45000);

// The gateway closes the socket after session.end, and Node surfaces
// that as an error event before the close. Only a failure BEFORE the
// socket ever opened means the gateway was unreachable.
ws.onerror = () => { if (!opened) fail(`could not reach ${WS} — is the gateway up?`); };

ws.onopen = async () => {
  opened = true;
  ws.send(encodeControl(seq++, {
    type: "session.start", mode: "online", sample_rate_hz: SAMPLE_RATE,
    encoding: "pcm_s16le", channels: 1, nominal_frame_ms: FRAME_SAMPLES / 16,
  }));

  // Paced at roughly real time: the VAD and the chunker are duration
  // driven, so blasting the clip would exercise a cadence no microphone
  // ever produces.
  for (const f of frames) {
    if (ws.readyState !== WebSocket.OPEN || errored) break;
    ws.send(encodeAudio(seq++, Date.now() - t0, f));
    await new Promise(r => setTimeout(r, FRAME_SAMPLES / 16));
  }
  if (!errored) ws.send(encodeControl(seq++, { type: "session.end" }));
};

ws.onmessage = (ev) => {
  const m = JSON.parse(ev.data);
  sessionId ||= m.session_id;
  if (m.type === "partial") {
    partials++;
    if (m.text && m.text !== lastPartial) { lastPartial = m.text; console.log(`  partial ${String(partials).padStart(3)}: ${m.text}`); }
  }
  if (m.type === "final") finalText = m.text || "";
  if (m.type === "error") { errored = m.reason; fail(`gateway rejected the frames: ${m.reason}`); }
  if (m.type === "overloaded") { errored = m.reason; fail(`session refused (retryable): ${m.reason}`); }
};

ws.onclose = () => {
  if (errored) fail(errored);
  if (finalText === null) fail("connection closed with no final");
  console.log(`session : ${sessionId}`);
  console.log(`partials: ${partials}`);
  console.log(`final   : "${finalText}"`);
  if (!finalText.trim()) fail("final was empty — frames were accepted but decoded to nothing");
  console.log("\nPASS: the mic page's own encoder produced frames this gateway accepted,");
  console.log("      and they decoded to a real transcript.");
  process.exit(0);
};
