import { Menu, Transition } from '@headlessui/react';
import { Fragment } from "react";
import clsx from "clsx";
import type { Theme } from "../state/session";

interface ToolbarProps {
  onRun(): void;
  onExplain(): void;
  onExport(): void;
  onOpen(): void;
  onCreate(): void;
  onSelectRecent(path: string): void;
  isRunning: boolean;
  isOpening: boolean;
  dbPath: string | null;
  recentFiles: string[];
  theme: Theme;
  onToggleTheme(): void;
}

export default function Toolbar({
  onRun,
  onExplain,
  onExport,
  onOpen,
  onCreate,
  onSelectRecent,
  isRunning,
  isOpening,
  dbPath,
  recentFiles,
  theme,
  onToggleTheme
}: ToolbarProps) {
  return (
    <div className="flex items-center justify-between border-b border-slate-200 bg-white px-4 py-2 dark:border-slate-700 dark:bg-slate-800">
      <div className="flex items-center gap-2">
        <button
          type="button"
          className={clsx(
            "rounded-md bg-brand-500 px-3 py-1 text-sm font-medium text-white shadow-sm transition hover:bg-brand-600",
            isRunning && "cursor-not-allowed opacity-70"
          )}
          onClick={onRun}
          disabled={isRunning}
        >
          {isRunning ? "Running…" : "Run"}
        </button>
        <button
          type="button"
          className="rounded-md border border-slate-300 px-3 py-1 text-sm font-medium text-slate-700 transition hover:bg-slate-100 dark:border-slate-600 dark:text-slate-100 dark:hover:bg-slate-700"
          onClick={onExplain}
          disabled={isRunning}
        >
          Explain
        </button>
        <button
          type="button"
          className="rounded-md border border-slate-300 px-3 py-1 text-sm font-medium text-slate-700 transition hover:bg-slate-100 dark:border-slate-600 dark:text-slate-100 dark:hover:bg-slate-700"
          onClick={onExport}
          disabled={isRunning}
        >
          Export CSV
        </button>
      </div>
      <div className="flex items-center gap-3">
        <Menu as="div" className="relative inline-block text-left">
          <div>
            <Menu.Button
              className="inline-flex items-center gap-2 rounded-md border border-slate-300 px-3 py-1 text-sm font-medium text-slate-700 transition hover:bg-slate-100 disabled:cursor-not-allowed disabled:opacity-60 dark:border-slate-600 dark:text-slate-100 dark:hover:bg-slate-700"
              disabled={isOpening}
            >
              <span className="font-semibold">Database</span>
              <span className="flex items-center gap-2 text-slate-500 dark:text-slate-300">
                {isOpening && <span className="h-2.5 w-2.5 animate-spin rounded-full border-2 border-brand-500 border-t-transparent" aria-hidden="true" />}
                {dbPath ? truncatePath(dbPath) : isOpening ? "Opening…" : "Open…"}
              </span>
            </Menu.Button>
          </div>
          <Transition
            as={Fragment}
            enter="transition ease-out duration-100"
            enterFrom="transform opacity-0 scale-95"
            enterTo="transform opacity-100 scale-100"
            leave="transition ease-in duration-75"
            leaveFrom="transform opacity-100 scale-100"
            leaveTo="transform opacity-0 scale-95"
          >
            <Menu.Items className="absolute right-0 z-10 mt-2 w-72 origin-top-right divide-y divide-slate-200 rounded-md bg-white shadow-lg ring-1 ring-black ring-opacity-5 focus:outline-none dark:divide-slate-700 dark:bg-slate-800">
              <div className="px-1 py-1">
                <Menu.Item>
                  {({ active }) => (
                    <button
                      type="button"
                      onClick={onCreate}
                      className={clsx(
                        "flex w-full items-center rounded-md px-2 py-2 text-sm",
                        active ? "bg-slate-100 dark:bg-slate-700" : "",
                        "text-left"
                      )}
                      disabled={isOpening}
                    >
                      File → New Database…
                    </button>
                  )}
                </Menu.Item>
                <Menu.Item>
                  {({ active }) => (
                    <button
                      type="button"
                      onClick={onOpen}
                      className={clsx(
                        "flex w-full items-center rounded-md px-2 py-2 text-sm",
                        active ? "bg-slate-100 dark:bg-slate-700" : "",
                        "text-left",
                        isOpening && "cursor-not-allowed opacity-70"
                      )}
                      disabled={isOpening}
                    >
                      File → Open…
                    </button>
                  )}
                </Menu.Item>
              </div>
              {recentFiles.length > 0 && (
                <div className="px-1 py-1">
                  <p className="px-2 pb-1 text-xs uppercase tracking-wide text-slate-400 dark:text-slate-500">Recent</p>
                  {recentFiles.map((item) => (
                    <Menu.Item key={item}>
                      {({ active }) => (
                        <button
                          type="button"
                          className={clsx(
                            "flex w-full items-center rounded-md px-2 py-2 text-xs text-slate-600 dark:text-slate-200",
                            active ? "bg-slate-100 dark:bg-slate-700" : "",
                            isOpening && "cursor-not-allowed opacity-60"
                          )}
                          onClick={() => onSelectRecent(item)}
                          disabled={isOpening}
                        >
                          {truncatePath(item)}
                        </button>
                      )}
                    </Menu.Item>
                  ))}
                </div>
              )}
            </Menu.Items>
          </Transition>
        </Menu>
        <button
          type="button"
          onClick={onToggleTheme}
          className="rounded-md border border-slate-300 px-3 py-1 text-sm font-medium text-slate-700 transition hover:bg-slate-100 dark:border-slate-600 dark:text-slate-100 dark:hover:bg-slate-700"
        >
          {theme === "dark" ? "Light Theme" : "Dark Theme"}
        </button>
      </div>
    </div>
  );
}

function truncatePath(path: string): string {
  if (path.length <= 40) {
    return path;
  }
  return `…${path.slice(-39)}`;
}
