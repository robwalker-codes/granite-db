import { Component, type ErrorInfo, type ReactNode } from "react";

interface AppErrorBoundaryProps {
  children: ReactNode;
}

interface AppErrorBoundaryState {
  hasError: boolean;
  message?: string;
}

export default class AppErrorBoundary extends Component<AppErrorBoundaryProps, AppErrorBoundaryState> {
  public state: AppErrorBoundaryState = { hasError: false };

  static getDerivedStateFromError(error: unknown): AppErrorBoundaryState {
    const message = error instanceof Error ? error.message : String(error);
    return { hasError: true, message };
  }

  componentDidCatch(error: unknown, info: ErrorInfo): void {
    console.error("[AppErrorBoundary]", error, info);
  }

  private handleRetry = (): void => {
    this.setState({ hasError: false, message: undefined });
  };

  private handleRestart = (): void => {
    window.location.reload();
  };

  override render(): ReactNode {
    if (this.state.hasError) {
      return (
        <div className="flex h-screen flex-col items-center justify-center gap-4 bg-slate-100 p-6 text-center dark:bg-slate-900">
          <div>
            <h1 className="text-2xl font-semibold text-slate-900 dark:text-slate-100">Something went wrong</h1>
            <p className="mt-2 max-w-xl text-sm text-slate-600 dark:text-slate-300">
              {this.state.message ?? "An unexpected error occurred. You can try again or restart the application."}
            </p>
          </div>
          <div className="flex gap-3">
            <button
              type="button"
              className="rounded bg-brand-600 px-4 py-2 text-sm font-medium text-white shadow hover:bg-brand-700 focus:outline-none focus:ring-2 focus:ring-brand-400"
              onClick={this.handleRetry}
            >
              Retry
            </button>
            <button
              type="button"
              className="rounded border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-200 focus:outline-none focus:ring-2 focus:ring-brand-400 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-700"
              onClick={this.handleRestart}
            >
              Restart
            </button>
          </div>
        </div>
      );
    }

    return this.props.children;
  }
}
