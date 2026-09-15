// A manual browser fixture using the real component. It only builds and serves;
// open the printed URL with the approved browser UI tool and click the buttons.
// DCAR_THUMBNAIL_GOOD_URL must be an already verified HTTPS cover, never committed.
import assert from "node:assert/strict";
import http from "node:http";
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";

const require = createRequire(import.meta.url);
const { build } = require("esbuild");
const good = process.env.DCAR_THUMBNAIL_GOOD_URL;
assert.ok(good && good.startsWith("https://"), "Set DCAR_THUMBNAIL_GOOD_URL to a verified HTTPS cover URL");
const webRoot = fileURLToPath(new URL("../", import.meta.url));
const compiled = await build({ absWorkingDir: webRoot, bundle: true, write: false,
  format: "iife", platform: "browser", jsx: "automatic",
  // Next's image module reads build-time environment keys at import, even when
  // platform marks are hidden. Supply a fixture-only browser process shim.
  define: { process: '{"env":{},"browser":true}', "process.env.NODE_ENV": '"development"', "process.env.NEXT_PUBLIC_DCAR_API_BASE": '""', "process.env.NEXT_PUBLIC_DCAR_BASE_PATH": '""' },
  stdin: { resolveDir: webRoot, loader: "jsx", contents: `
    import React, {useEffect, useState} from "react";
    import {createRoot} from "react-dom/client";
    import ContentMediaBox from "./app/contents/ContentMediaBox";
    const good=${JSON.stringify(good)};
    const failed=["https://127.0.0.1/missing-thumb-one.webp", "https://127.0.0.1/missing-thumb-two.webp", "https://127.0.0.1/missing-thumb-three.webp"];
    const item={id:1,title:"快手封面换源验收",platform:"kuaishou",content_type:"video",local_media_available:false,canonical_url:"https://www.kuaishou.com/short-video/fixture"};
    function App() {
      const [thumbnail,setThumbnail]=useState({remote_url:null,reason:"not_found"});
      const [events,setEvents]=useState([]);
      const [observed,setObserved]=useState({source:"",width:0,loaded:false,hint:""});
      const [opened,setOpened]=useState(0);
      const [local,setLocal]=useState(false);
      useEffect(()=>{
        const scan=()=>{
          const image=document.querySelector(".content-thumbnail-image");
          const box=document.querySelector(".content-media-box");
          const value={source:image?.getAttribute("src")??"",host:image?new URL(image.src).host:"",width:image?.naturalWidth??0,loaded:image?.getAttribute("data-loaded")==="true",hint:box?.title??""};
          setObserved(previous=>JSON.stringify(previous)===JSON.stringify(value)?previous:value);
        };
        const event=(event)=>{
          const target=event.target;
          if(!target?.matches?.(".content-thumbnail-image"))return;
          // Native event targets may be cleared after dispatch; React can defer
          // state updaters, so snapshot the event before scheduling the update.
          const url=new URL(target.src);
          const record={event:event.type,host:url.host,path:url.pathname,width:target.naturalWidth};
          setEvents(previous=>[...previous,record]);
          scan();
        };
        const observer=new MutationObserver(scan);
        observer.observe(document.getElementById("sample"),{subtree:true,childList:true,attributes:true,attributeFilter:["src","data-loaded","title"]});
        document.addEventListener("load",event,true);document.addEventListener("error",event,true);
        scan();return()=>{observer.disconnect();document.removeEventListener("load",event,true);document.removeEventListener("error",event,true);};
      },[]);
      function choose(name) {
        setEvents([]);
        if(name==="fallback")setThumbnail({remote_url:failed[0],remote_urls:[failed[0],failed[1],good]});
        if(name==="fail")setThumbnail({remote_url:failed[0],remote_urls:failed});
        if(name==="recover")setThumbnail({remote_url:failed[0],remote_urls:[failed[0],good]});
        if(name==="legacy")setThumbnail({remote_url:good});
        if(name==="same")setThumbnail(previous=>({...previous,remote_urls:previous.remote_urls?[...previous.remote_urls]:undefined}));
        if(name==="missing")setThumbnail({remote_url:null,reason:"not_found"});
        if(name==="unsupported")setThumbnail({remote_url:null,reason:"unsupported_format"});
        if(name==="unavailable")setThumbnail({remote_url:null,reason:"source_unavailable"});
      }
      return <main><h1>快手封面换源验收</h1><p>组件会直接请求图片；下方记录真实浏览器 load/error 事件。先点全部失败，再点同首地址更新备用，检查恢复。</p>
        <nav>{[["fallback","两次失败后备用成功"],["fail","全部失败（最多三次）"],["same","相同候选重新渲染"],["recover","同首地址更新备用恢复"],["legacy","旧接口单地址"],["missing","未取得封面"],["unsupported","格式不支持"],["unavailable","资料不可用"]].map(([name,label])=><button key={name} onClick={()=>choose(name)}>{label}</button>)}</nav>
        <label><input type="checkbox" checked={local} onChange={event=>setLocal(event.target.checked)}/>本地媒体点击行为</label><output id="opened">播放点击次数：{opened}</output>
        <div id="sample"><ContentMediaBox item={{...item,local_media_available:local}} thumbnail={thumbnail} onOpen={()=>setOpened(value=>value+1)} showPlatformMark={false}/></div>
        <section><h2>当前图片</h2><dl><dt>地址域名</dt><dd id="source-host">{observed.host||"无图片"}</dd><dt>实际宽度</dt><dd id="natural-width">{observed.width}</dd><dt>加载成功</dt><dd id="loaded">{String(observed.loaded)}</dd><dt>悬停提示</dt><dd id="hint">{observed.hint}</dd></dl></section>
        <section><h2>本次真实图片事件（{events.length}）</h2><ol id="events">{events.map((event,index)=><li key={index}>{event.event} | {event.host}{event.path} | width={event.width}</li>)}</ol></section>
      </main>;
    }
    createRoot(document.getElementById("root")).render(<App/>);` },
});
const page = `<!doctype html><html lang="zh"><meta charset="utf-8"><title>Thumbnail browser fixture</title><style>
body{font-family:system-ui;background:#f6f8fa;color:#22363e;margin:0}main{padding:24px;max-width:1040px}nav{display:flex;gap:8px;flex-wrap:wrap;margin:16px 0}button{cursor:pointer;padding:8px;border:1px solid #bdcdd3;border-radius:6px;background:white}label,output{display:block;margin:8px 0}#sample{padding:20px 0}.content-media-box{display:grid;place-items:center;position:relative;width:240px;height:150px;border:1px solid #bccbd0;border-radius:8px;background:#e8edf0;color:#33525e;text-decoration:none}.content-thumbnail{position:absolute;inset:0;width:100%;height:100%;object-fit:cover;border-radius:inherit;pointer-events:none}.content-media-glyph{z-index:1;background:#3458;color:white;padding:10px;border-radius:50%;display:grid}.content-media-glyph svg{width:24px;height:24px}.content-media-label{position:absolute;clip-path:inset(50%);height:1px}.content-media-badge{position:absolute;right:4px;top:4px;color:white}section{background:white;border:1px solid #dde5e8;padding:12px;margin:10px 0}h2{font-size:16px}dl{display:grid;grid-template-columns:120px 1fr;gap:6px}dd{margin:0}li{overflow-wrap:anywhere;font-family:monospace;font-size:12px}
</style><div id="root"></div><script src="/bundle.js"></script></html>`;
const server = http.createServer((request, response) => {
  if (request.url === "/bundle.js") { response.setHeader("content-type", "text/javascript; charset=utf-8"); response.end(compiled.outputFiles[0].text); }
  else { response.setHeader("content-type", "text/html; charset=utf-8"); response.end(page); }
});
server.listen(0, "127.0.0.1", () => console.log(`Thumbnail fixture: http://127.0.0.1:${server.address().port}`));
for (const signal of ["SIGINT", "SIGTERM"]) process.once(signal, () => server.close());
