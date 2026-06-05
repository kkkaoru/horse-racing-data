// Run with: bunx vitest run src/app/races/detail/mobile-collapsible-section.test.tsx

"use client";

import type { ReactNode } from "react";
import { useEffect, useId, useState } from "react";

interface MobileCollapsibleSectionProps {
  children: ReactNode;
  heading?: ReactNode;
  title: string;
}

const MOBILE_QUERY = "(max-width: 720px)";
const OPEN_LABEL = "閉じる";
const CLOSED_LABEL = "開く";
const TOGGLE_ARIA_LABEL_OPEN = "セクションを閉じる";
const TOGGLE_ARIA_LABEL_CLOSED = "セクションを開く";

const resolveToggleLabel = (expanded: boolean): string => (expanded ? OPEN_LABEL : CLOSED_LABEL);

const resolveToggleAriaLabel = (params: { expanded: boolean; title: string }): string =>
  params.expanded
    ? `${params.title} ${TOGGLE_ARIA_LABEL_OPEN}`
    : `${params.title} ${TOGGLE_ARIA_LABEL_CLOSED}`;

export function MobileCollapsibleSection({
  children,
  heading,
  title,
}: MobileCollapsibleSectionProps) {
  const [mobile, setMobile] = useState(false);
  const [open, setOpen] = useState(false);
  const bodyId = useId();
  const expanded = mobile ? open : true;
  const handleToggle = () => {
    if (!mobile) {
      return;
    }
    setOpen((current) => !current);
  };

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
    if (mediaQuery.addEventListener) {
      mediaQuery.addEventListener("change", updateMode);
      return () => {
        mediaQuery.removeEventListener("change", updateMode);
      };
    }
    mediaQuery.addListener(updateMode);
    return () => {
      mediaQuery.removeListener(updateMode);
    };
  }, []);

  return (
    <div className="mobile-collapsible-section">
      <button
        aria-controls={bodyId}
        aria-expanded={expanded}
        aria-label={resolveToggleAriaLabel({ expanded, title })}
        className="mobile-collapsible-section-toggle"
        type="button"
        onClick={handleToggle}
      >
        <span className="mobile-collapsible-section-toggle-title">{title}</span>
        <span aria-hidden="true" className="mobile-collapsible-section-toggle-state">
          {resolveToggleLabel(expanded)}
        </span>
      </button>
      {heading === undefined ? null : (
        <div className="mobile-collapsible-section-desktop-heading">{heading}</div>
      )}
      <div className="mobile-collapsible-section-body" hidden={!expanded} id={bodyId}>
        {children}
      </div>
    </div>
  );
}
