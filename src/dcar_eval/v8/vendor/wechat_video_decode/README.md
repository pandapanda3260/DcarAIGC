# Pinned offline video decoder

TikHub's [official video detail contract](https://docs.tikhub.io/472974842e0)
links the [Evil0ctal decoder](https://github.com/Evil0ctal/WeChat-Channels-Video-File-Decryption).
The unmodified generated JavaScript and WASM are pinned to commit
`799a131834ccc6ac69d1fbfa8c30350c4f718ccb`, under its accompanying MIT license.
The WASM also matches the gzip-decoded bytes from Tencent's
`aladin.wxqcloud.qq.com/aladin/ffmepeg/video-decode/1.2.46/wasm_video_decode.wasm`.

SHA-256: JS `78faf7621959e30ba05c0acf7182dd14bcbd7dfe45529476649f39b20e5dbea3`;
WASM `dca796bacec37d8522c7983b3945e5d579bd74164e3b21f0ebc773be6dfc8b6e`.

`keystream.cjs` exposes no networking capability to the generated glue. It uses
only the bundled bytes, receives the exact decimal key via stdin, emits the
128 KiB reversed WxIsaac64 byte stream, and exits. Python applies XOR only to
the first 128 KiB of the already bounded private download spool. A validated
source raw response provides URL/token/key as one immutable binding. Neither
the private key nor key stream is exposed in evidence metadata or API output.

Requires a local Node runtime on the writer PATH. Missing Node or mismatched
vendor bytes yields `decryption_runtime_unavailable`; no provider call is made.
There is no runtime download or unpinned package installation.

On 2026-09-12 the pinned repository's real public `wx_encrypted.mp4` fixture
(Git blob `33d3e2d7fdcd29197ed457339402be8a03aae46b`, 14,088,528 bytes) and
matching `wx_response.json` were validated through the bounded downloader.
Encrypted SHA-256: `b574ce16f21e8e78060c39f218128ff7c0ff659306e8fd61eb3d8600b1cf44fc`.
Decrypted SHA-256: `37d222b744b68a71a8bed4843c46bbffd17ce259fc9078cb9e7070610a99039a`.
The 52.314671-second output passed full `ffmpeg -xerror` video/audio decoding.
The public fixture video is not distributed with this application. Small
offline test vectors guard the decoder and publication checks.
