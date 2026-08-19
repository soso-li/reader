"use client";

import { FormEvent, useEffect, useRef, useState, useTransition } from "react";
import { useRouter } from "next/navigation";
import { Search, X } from "lucide-react";

import { queryString } from "./url-state";

type Scope = Record<string, string | number | null | undefined>;
export type SearchNavigation = { href: string; query: string };

export default function SearchBox({ onNavigate, pending: externalPending = false, placeholder, query, scope }: { onNavigate?: (navigation: SearchNavigation) => void; pending?: boolean; placeholder: string; query: string; scope: Scope }) {
  const router = useRouter();
  const [expanded, setExpanded] = useState(Boolean(query));
  const [value, setValue] = useState(query);
  const [pending, startTransition] = useTransition();
  const busy = pending || externalPending;
  const inputRef = useRef<HTMLInputElement | null>(null);

  useEffect(() => {
    setValue(query);
    setExpanded(Boolean(query));
  }, [query]);

  useEffect(() => {
    function focusSearch() {
      setExpanded(true);
      window.setTimeout(() => inputRef.current?.focus(), 0);
    }

    function collapseSearch() {
      if (!expanded) return;
      setExpanded(false);
      inputRef.current?.blur();
    }

    window.addEventListener("reader:focus-search", focusSearch);
    window.addEventListener("reader:collapse-search", collapseSearch);
    return () => {
      window.removeEventListener("reader:focus-search", focusSearch);
      window.removeEventListener("reader:collapse-search", collapseSearch);
    };
  }, [expanded]);

  function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    navigate(value);
  }

  function navigate(nextQuery: string) {
    const href = searchHref(scope, nextQuery);
    if (onNavigate) onNavigate({ href, query: nextQuery.trim() });
    else startTransition(() => router.push(href));
  }

  return (
    <form className={`search-box ${expanded ? "expanded" : ""} ${busy ? "pending" : ""}`} aria-busy={busy} onSubmit={submit}>
      <button
        className="icon"
        type="button"
        title="搜索"
        aria-label="搜索"
        onClick={() => {
          setExpanded(true);
          window.setTimeout(() => inputRef.current?.focus(), 0);
        }}
      >
        <Search size={17} />
      </button>
      {expanded ? (
        <div className="search-panel">
          <input
            id="reader-search-input"
            ref={inputRef}
            aria-label={placeholder}
            name="q"
            value={value}
            placeholder={placeholder}
            onChange={(event) => setValue(event.target.value)}
            onKeyDown={(event) => {
              if (event.key === "Enter" && !event.nativeEvent.isComposing) {
                event.preventDefault();
                event.currentTarget.form?.requestSubmit();
              }
            }}
          />
          {value ? (
            <button
              className="icon"
              type="button"
              title="清空搜索"
              aria-label="清空搜索"
              onClick={() => {
                setValue("");
                if (query.trim()) navigate("");
              }}
            >
              <X size={15} />
            </button>
          ) : null}
          <span className="search-status" role="status" aria-live="polite">{busy ? "正在搜索…" : ""}</span>
        </div>
      ) : null}
    </form>
  );
}

function searchHref(scope: Scope, q: string) {
  const query = q.trim();
  return `/?${queryString({ ...scope, q: query || undefined, offset: undefined, item_id: undefined, cluster_id: undefined })}`;
}
