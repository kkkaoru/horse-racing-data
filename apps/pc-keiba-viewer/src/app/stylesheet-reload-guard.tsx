"use client";

// Recovers from "page restored without styles" in iOS Safari after a few days
// idle. Two failure modes are covered:
//   1. bfcache restore: Safari resurrects the prior HTML but the CSS link
//      points to a `/_next/static/css/<hash>.css` that has since been
//      replaced by a newer deploy and now returns 404. The link sits in the
//      document with no `.sheet`, so the page renders unstyled.
//   2. First navigation after a long idle: HTML is fetched fresh, but the
//      CSS link errors out (deploy raced, asset evicted, etc). Reloading
//      once fetches the latest HTML which points at current asset hashes.
//
// Execute with bun: opennextjs-cloudflare build && wrangler dev

import { useEffect } from "react";

const RELOAD_GUARD_KEY = "pc-keiba-stylesheet-reload-attempted";
const STYLESHEET_LINK_SELECTOR = 'link[rel="stylesheet"]';

const isStylesheetLink = (target: EventTarget | null): target is HTMLLinkElement =>
  target instanceof HTMLLinkElement && target.rel === "stylesheet";

const stylesheetLoaded = (link: HTMLLinkElement): boolean => {
  try {
    return link.sheet !== null;
  } catch {
    return true;
  }
};

const hasMissingStylesheet = (root: Document): boolean =>
  Array.from(root.querySelectorAll<HTMLLinkElement>(STYLESHEET_LINK_SELECTOR)).some(
    (link) => !stylesheetLoaded(link),
  );

const reloadOnce = (): void => {
  if (typeof sessionStorage === "undefined") {
    window.location.reload();
    return;
  }
  if (sessionStorage.getItem(RELOAD_GUARD_KEY) === "1") {
    return;
  }
  sessionStorage.setItem(RELOAD_GUARD_KEY, "1");
  window.location.reload();
};

const clearReloadGuard = (): void => {
  if (typeof sessionStorage !== "undefined") {
    sessionStorage.removeItem(RELOAD_GUARD_KEY);
  }
};

export function StylesheetReloadGuard() {
  useEffect(() => {
    const handlePageShow = (event: PageTransitionEvent): void => {
      if (!event.persisted) {
        return;
      }
      if (hasMissingStylesheet(document)) {
        reloadOnce();
      }
    };

    const handleStylesheetError = (event: Event): void => {
      if (isStylesheetLink(event.target)) {
        reloadOnce();
      }
    };

    // Only clear the reload guard once we've confirmed the live document
    // has its stylesheets — otherwise we'd race against a still-broken
    // bfcache restore and allow infinite reload loops.
    if (!hasMissingStylesheet(document)) {
      clearReloadGuard();
    }
    window.addEventListener("pageshow", handlePageShow);
    document.addEventListener("error", handleStylesheetError, true);
    return () => {
      window.removeEventListener("pageshow", handlePageShow);
      document.removeEventListener("error", handleStylesheetError, true);
    };
  }, []);

  return null;
}
