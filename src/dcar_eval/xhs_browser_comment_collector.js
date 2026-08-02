/*
 * Xiaohongshu logged-in browser comment collector.
 *
 * Run this file as a Chrome DevTools Snippet on an already-open note page, then:
 *
 *   await xhsBrowserCommentCollector.collect({ targetUniqueUsers: 30 })
 *
 * The collector never exports cookies, request headers, profile URLs, nicknames,
 * locations, or the xsec_token. Raw profile identity is held only in memory long
 * enough to assign run-local U001/U002-style keys.
 */
(function installXhsBrowserCommentCollector(globalObject) {
  "use strict";

  const VERSION = "xhs-browser-dom-v1";
  const DEFAULT_OPTIONS = Object.freeze({
    targetUniqueUsers: 30,
    maxScrolls: 20,
    settleMs: 1200,
    stableRoundsToStop: 2,
  });

  function normalizeText(value) {
    return String(value || "").replace(/\s+/g, " ").trim();
  }

  function parseNoteId(pathname) {
    const match = String(pathname || "").match(
      /\/(?:explore|discovery\/item)\/([0-9a-f]{24})(?:\/|$)/i,
    );
    return match ? match[1].toLowerCase() : null;
  }

  function parsePlatformCommentCount(value) {
    const match = String(value || "").match(/([\d,]+)\s*条评论/);
    return match ? Number(match[1].replace(/,/g, "")) : null;
  }

  function publicSourceUrl(value) {
    try {
      const url = new URL(String(value));
      return `${url.origin}${url.pathname}`;
    } catch (_error) {
      return null;
    }
  }

  function commentHeading(root) {
    const container = root.querySelector(".comments-container");
    return normalizeText(container?.innerText?.split("\n")[0]);
  }

  function nextUserKey(state, transientIdentity) {
    if (!state.userOrdinals.has(transientIdentity)) {
      const value = `U${String(state.nextUserOrdinal).padStart(3, "0")}`;
      state.userOrdinals.set(transientIdentity, value);
      state.nextUserOrdinal += 1;
    }
    return state.userOrdinals.get(transientIdentity);
  }

  function captureVisibleComments(root, state) {
    const items = Array.from(root.querySelectorAll(".comment-item"));
    const captured = [];

    for (let index = 0; index < items.length; index += 1) {
      const item = items[index];
      const text = normalizeText(item.querySelector(".content")?.innerText);
      if (!text) continue;

      const nameElement = item.querySelector(".author .name");
      const transientIdentity =
        nameElement?.getAttribute("href") ||
        normalizeText(nameElement?.textContent) ||
        `anonymous:${index}:${text}`;
      const userKey = nextUserKey(state, transientIdentity);
      const level = item.classList.contains("comment-item-sub") ? 2 : 1;
      const authorText = normalizeText(
        item.querySelector(".author-wrapper")?.innerText,
      );
      const isNoteAuthor = /作者/.test(authorText);
      const domCommentId =
        item.getAttribute("data-comment-id") || item.getAttribute("id") || "";
      const dedupeKey = domCommentId || `${transientIdentity}\u0000${level}\u0000${text}`;

      captured.push({
        dedupeKey,
        level,
        user_key: userKey,
        text,
        is_note_author: isNoteAuthor,
      });
    }
    return captured;
  }

  function sanitizedVisibleSnapshot(root) {
    const state = { userOrdinals: new Map(), nextUserOrdinal: 1 };
    return captureVisibleComments(root, state).map((record, index) => ({
      sequence: index + 1,
      level: record.level,
      user_key: record.user_key,
      text: record.text,
      is_note_author: record.is_note_author,
    }));
  }

  function delay(milliseconds) {
    return new Promise((resolve) => globalObject.setTimeout(resolve, milliseconds));
  }

  async function collect(options = {}) {
    const settings = { ...DEFAULT_OPTIONS, ...options };
    const root = globalObject.document;
    const location = globalObject.location;

    if (!root || !location || !/xiaohongshu\.com$/i.test(location.hostname)) {
      throw new Error("Open a Xiaohongshu note page before running the collector.");
    }

    const noteId = parseNoteId(location.pathname);
    if (!noteId) {
      throw new Error("The current page is not a supported Xiaohongshu note URL.");
    }

    const scroller = root.querySelector(".note-scroller");
    const commentsContainer = root.querySelector(".comments-container");
    if (!scroller || !commentsContainer) {
      throw new Error(
        "The note or comment area is not ready. Confirm login and wait for comments to render.",
      );
    }

    const state = { userOrdinals: new Map(), nextUserOrdinal: 1 };
    const accumulated = new Map();
    const platformCommentCount = parsePlatformCommentCount(commentHeading(root));
    const initialVisible = captureVisibleComments(root, state);
    for (const record of initialVisible) accumulated.set(record.dedupeKey, record);

    let scrolls = 0;
    let stableRounds = 0;
    let paginationObserved = false;
    let previousHeight = scroller.scrollHeight;
    let previousCount = accumulated.size;
    let stopReason = "target_reached";

    while (scrolls < settings.maxScrolls) {
      const validUsers = new Set(
        Array.from(accumulated.values())
          .filter((record) => !record.is_note_author)
          .map((record) => record.user_key),
      );
      if (validUsers.size >= settings.targetUniqueUsers) break;

      scroller.scrollTop = scroller.scrollHeight;
      scroller.dispatchEvent(new Event("scroll", { bubbles: true }));
      scrolls += 1;
      await delay(settings.settleMs);

      const visible = captureVisibleComments(root, state);
      for (const record of visible) accumulated.set(record.dedupeKey, record);

      const currentHeight = scroller.scrollHeight;
      const currentCount = accumulated.size;
      if (currentCount > previousCount || currentHeight > previousHeight) {
        paginationObserved = true;
        stableRounds = 0;
      } else {
        stableRounds += 1;
      }
      previousHeight = currentHeight;
      previousCount = currentCount;

      if (stableRounds >= settings.stableRoundsToStop) {
        stopReason = "stable_bottom";
        break;
      }
      stopReason = "max_scrolls";
    }

    const records = Array.from(accumulated.values()).map((record, index) => ({
      sequence: index + 1,
      level: record.level,
      user_key: record.user_key,
      text: record.text,
      is_note_author: record.is_note_author,
    }));
    const uniqueExternalUsers = new Set(
      records
        .filter((record) => !record.is_note_author)
        .map((record) => record.user_key),
    );

    if (uniqueExternalUsers.size >= settings.targetUniqueUsers) {
      stopReason = "target_reached";
    }

    return {
      schema_version: VERSION,
      note_id: noteId,
      source_url: publicSourceUrl(location.href),
      title: String(root.title || "").replace(/\s*-\s*小红书\s*$/, ""),
      status: records.length ? "success" : "not_retrieved",
      platform_comment_count: platformCommentCount,
      initial_visible_comment_count: initialVisible.length,
      retrieved_comment_count: records.length,
      unique_external_user_count: uniqueExternalUsers.size,
      scrolls,
      pagination_observed: paginationObserved,
      pagination_complete:
        Number.isFinite(platformCommentCount) && records.length >= platformCommentCount,
      stop_reason: stopReason,
      records,
    };
  }

  const api = Object.freeze({
    VERSION,
    collect,
    normalizeText,
    parseNoteId,
    parsePlatformCommentCount,
    publicSourceUrl,
    sanitizedVisibleSnapshot,
  });

  globalObject.xhsBrowserCommentCollector = api;
  if (typeof module !== "undefined" && module.exports) module.exports = api;
})(globalThis);
