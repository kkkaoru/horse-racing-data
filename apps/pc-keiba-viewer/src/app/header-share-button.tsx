"use client";

import { usePathname } from "next/navigation";
import { useEffect, useState } from "react";

import {
  buildShareUrl,
  DEFAULT_SHARE_HOURS,
  readShareDeadline,
  SHARE_HOUR_OPTIONS,
  writeShareDeadline,
} from "../lib/share-link";

const HOUR_MS = 3_600_000;
const NOTICE_RESET_MS = 2400;

export function HeaderShareButton() {
  const pathname = usePathname();
  const [hours, setHours] = useState<number>(DEFAULT_SHARE_HOURS);
  const [deadline, setDeadline] = useState(0);
  const [notice, setNotice] = useState("");

  useEffect(() => {
    setDeadline(readShareDeadline(pathname));
    setNotice("");
  }, [pathname]);

  const share = () => {
    void (async () => {
      const nextDeadline = Date.now() + hours * HOUR_MS;
      try {
        await navigator.clipboard.writeText(buildShareUrl(pathname, window.location.search));
      } catch {
        setNotice("共有できませんでした");
        return;
      }
      writeShareDeadline(pathname, nextDeadline);
      setDeadline(nextDeadline);
      setNotice("共有しました");
      window.setTimeout(() => {
        setNotice("");
      }, NOTICE_RESET_MS);
    })();
  };

  return (
    <>
      {deadline > 0 ? (
        <button
          className="race-share-button"
          disabled
          title={`共有期限: ${new Date(deadline).toLocaleString("ja-JP")}`}
          type="button"
        >
          共有済み
        </button>
      ) : (
        <>
          <select
            aria-label="共有期限"
            className="race-share-button"
            value={hours}
            onChange={(event) => {
              setHours(Number(event.target.value));
            }}
          >
            {SHARE_HOUR_OPTIONS.map((option) => (
              <option key={option} value={option}>
                {`共有期限 ${option}時間`}
              </option>
            ))}
          </select>
          <button className="race-share-button" type="button" onClick={share}>
            共有
          </button>
        </>
      )}
      {notice === "" ? null : <output className="header-share-notice">{notice}</output>}
    </>
  );
}
