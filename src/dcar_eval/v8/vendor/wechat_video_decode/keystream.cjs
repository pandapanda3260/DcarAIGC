// Offline bridge to the supplier-linked, pinned official WxIsaac64 module.
// The key arrives on stdin, never in command arguments, logs or a remote API.
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");
const crypto = require("node:crypto");
const load = (name, expected) => {
  const bytes = fs.readFileSync(path.join(__dirname, name));
  if (crypto.createHash("sha256").update(bytes).digest("hex") !== expected) throw new Error("decoder asset drift");
  return bytes;
};
const binary = load("wasm_video_decode.wasm", "dca796bacec37d8522c7983b3945e5d579bd74164e3b21f0ebc773be6dfc8b6e");
const source = load("wasm_video_decode.js", "78faf7621959e30ba05c0acf7182dd14bcbd7dfe45529476649f39b20e5dbea3");
const key = fs.readFileSync(0, "utf8").trim();
if (!/^[0-9]{1,20}$/.test(key) || BigInt(key) > 18446744073709551615n) throw new Error("invalid decoder key");
let generated = false;
const fail = () => { process.stderr.write("decryption runtime failed\n"); process.exitCode = 1; };
const context = {
  console: { log() {}, warn() {}, error() {} }, Uint8Array, WebAssembly, TextDecoder, TextEncoder,
  self: { location: { href: "file:///offline/worker.js" } },
  VTS_WASM_URL: "file:///offline/wasm_video_decode.wasm", MAX_HEAP_SIZE: 33554432,
  Module: { wasmBinary: binary, print() {}, printErr() {}, onAbort: fail,
    onRuntimeInitialized() {
      try {
        const decoder = new context.Module.WxIsaac64(key);
        try { decoder.generate(131072); } finally { decoder.delete(); }
        if (!generated) fail();
      } catch { fail(); }
    },
  },
  wasm_isaac_generate(ptr, size) {
    if (generated || size !== 131072 || ptr < 0 || ptr + size > context.Module.HEAPU8.length) throw new Error("invalid keystream");
    const result = Buffer.from(new Uint8Array(context.Module.HEAPU8.buffer, ptr, size));
    result.reverse();
    generated = true;
    process.stdout.write(result);
  },
};
// No fetch, XMLHttpRequest, require, process or filesystem is exposed to WASM glue.
vm.createContext(context);
try { vm.runInContext(source.toString("utf8"), context, { timeout: 30000 }); } catch { fail(); }
