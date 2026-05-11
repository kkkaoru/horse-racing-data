"use client";

import type { ReactNode } from "react";
import { useEffect, useState } from "react";

interface MobileFilterDisclosureProps {
  children: ReactNode;
  title: string;
}

const MOBILE_QUERY = "(max-width: 760px)";

export function MobileFilterDisclosure({ children, title }: MobileFilterDisclosureProps) {
  const [mobile, setMobile] = useState(false);
  const [open, setOpen] = useState(false);
  const expanded = mobile ? open : true;

  useEffect(() => {
    if (!window.matchMedia) {
      setMobile(false);
      setOpen(true);
      return undefined;
    }

    const mediaQuery = window.matchMedia(MOBILE_QUERY);
    const updateMode = () => {
      setMobile(mediaQuery.matches);
      setOpen(!mediaQuery.matches);
    };

    updateMode();
    mediaQuery.addEventListener("change", updateMode);
    return () => {
      mediaQuery.removeEventListener("change", updateMode);
    };
  }, []);

  return (
    <div className="mobile-filter-disclosure">
      <button
        aria-expanded={expanded}
        className="mobile-filter-disclosure-toggle"
        type="button"
        onClick={() => {
          if (mobile) {
            setOpen((current) => !current);
          }
        }}
      >
        <span>{title}</span>
        <span aria-hidden="true">{expanded ? "閉じる" : "開く"}</span>
      </button>
      <div className="mobile-filter-disclosure-body" hidden={!expanded}>
        {children}
      </div>
    </div>
  );
}
