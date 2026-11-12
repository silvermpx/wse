// =============================================================================
// WebSocket Engine - Logger
// =============================================================================

export enum LogLevel {
  DEBUG = 0,
  INFO = 1,
  WARN = 2,
  ERROR = 3,
  NONE = 4,
}

export interface LogEntry {
  level: LogLevel;
  timestamp: Date;
  message: string;
  context?: string;
  data?: any;
  error?: Error;
}

/** localStorage key of the opt-in persisted error log (cleared by clearWSEStorage()). */
export const ERROR_LOG_STORAGE_KEY = 'wse_error_logs';

/**
 * Level used when neither WSE_LOG_LEVEL nor VITE_LOG_LEVEL is set: INFO in
 * development, WARN in production (per-message INFO logging costs main-thread
 * time at high message rates).
 */
export function defaultLogLevel(isDevelopment: boolean): LogLevel {
  return isDevelopment ? LogLevel.INFO : LogLevel.WARN;
}

function detectDevelopment(): boolean {
  try {
    if (typeof import.meta !== 'undefined' && (import.meta as any).env?.DEV) return true;
  } catch {
    // import.meta.env is a bundler convention; absent in plain Node.
  }
  return typeof process !== 'undefined' && process.env?.NODE_ENV === 'development';
}

function detectEnvLevel(): string | undefined {
  try {
    const viteLevel = typeof import.meta !== 'undefined' ? (import.meta as any).env?.VITE_LOG_LEVEL : undefined;
    if (viteLevel) return viteLevel;
  } catch {
    // ignore
  }
  return typeof process !== 'undefined' ? process.env?.WSE_LOG_LEVEL : undefined;
}

class Logger {
  private level: LogLevel;
  private context: string;
  private history: LogEntry[] = [];
  private maxHistorySize = 1000;
  private isDevelopment: boolean;
  // Persisting error entries to localStorage is opt-in: they used to be written
  // by default together with their data (e.g. a failed send's payload) and
  // survived logout on shared browsers.
  private persistErrors = false;

  constructor(context: string = 'WSE') {
    this.context = context;

    // Detect development mode from common bundler conventions
    this.isDevelopment = detectDevelopment();

    this.level = defaultLogLevel(this.isDevelopment);

    // Set log level from environment if available
    const envLevel = detectEnvLevel();
    if (envLevel && envLevel in LogLevel && Number.isNaN(Number(envLevel))) {
      this.level = LogLevel[envLevel as keyof typeof LogLevel];
    }
  }

  // ---------------------------------------------------------------------------
  // Logging Methods
  // ---------------------------------------------------------------------------

  debug(message: string, data?: any): void {
    this.log(LogLevel.DEBUG, message, data);
  }

  info(message: string, data?: any): void {
    this.log(LogLevel.INFO, message, data);
  }

  warn(message: string, data?: any): void {
    this.log(LogLevel.WARN, message, data);
  }

  error(message: string, error?: Error | any, data?: any): void {
    if (error instanceof Error) {
      this.log(LogLevel.ERROR, message, { ...data, error: error.message, stack: error.stack }, error);
    } else {
      this.log(LogLevel.ERROR, message, { ...data, error });
    }
  }

  // ---------------------------------------------------------------------------
  // Core Logging
  // ---------------------------------------------------------------------------

  private log(level: LogLevel, message: string, data?: any, error?: Error): void {
    if (level < this.level) return;

    const entry: LogEntry = {
      level,
      timestamp: new Date(),
      message,
      context: this.context,
      data,
      error,
    };

    this.addToHistory(entry);

    const formattedMessage = this.formatMessage(entry);

    switch (level) {
      case LogLevel.DEBUG:
        data !== undefined ? console.debug(formattedMessage, data) : console.debug(formattedMessage);
        break;
      case LogLevel.INFO:
        data !== undefined ? console.info(formattedMessage, data) : console.info(formattedMessage);
        break;
      case LogLevel.WARN:
        data !== undefined ? console.warn(formattedMessage, data) : console.warn(formattedMessage);
        break;
      case LogLevel.ERROR:
        if (data !== undefined && error) {
          console.error(formattedMessage, data, error);
        } else if (data !== undefined) {
          console.error(formattedMessage, data);
        } else if (error) {
          console.error(formattedMessage, error);
        } else {
          console.error(formattedMessage);
        }
        break;
    }

    if (this.persistErrors && level >= LogLevel.ERROR) {
      this.sendToLoggingService(entry);
    }
  }

  private formatMessage(entry: LogEntry): string {
    const timestamp = entry.timestamp.toISOString();
    const level = LogLevel[entry.level];
    return `[${timestamp}] [${level}] [${entry.context}] ${entry.message}`;
  }

  private addToHistory(entry: LogEntry): void {
    this.history.push(entry);

    if (this.history.length > this.maxHistorySize) {
      this.history = this.history.slice(-this.maxHistorySize);
    }
  }

  // ---------------------------------------------------------------------------
  // Configuration
  // ---------------------------------------------------------------------------

  setLevel(level: LogLevel): void {
    this.level = level;
  }

  getLevel(): LogLevel {
    return this.level;
  }

  /**
   * Opt in to keeping the last 100 ERROR entries in localStorage
   * (`wse_error_logs`). Only level, time, context and message are stored --
   * never the attached data, which can hold message payloads.
   */
  setPersistErrors(enabled: boolean): void {
    this.persistErrors = enabled;
  }

  /** Remove the persisted error log from localStorage. */
  clearPersistedErrors(): void {
    try {
      if (typeof localStorage !== 'undefined') {
        localStorage.removeItem(ERROR_LOG_STORAGE_KEY);
      }
    } catch {
      // Ignore storage errors
    }
  }

  setContext(context: string): void {
    this.context = context;
  }

  child(context: string): Logger {
    const childLogger = new Logger(`${this.context}:${context}`);
    childLogger.setLevel(this.level);
    return childLogger;
  }

  // ---------------------------------------------------------------------------
  // History & Diagnostics
  // ---------------------------------------------------------------------------

  getHistory(level?: LogLevel): LogEntry[] {
    if (level !== undefined) {
      return this.history.filter(entry => entry.level === level);
    }
    return [...this.history];
  }

  clearHistory(): void {
    this.history = [];
  }

  exportHistory(): string {
    return this.history
      .map(entry => {
        const message = this.formatMessage(entry);
        if (entry.data) {
          return `${message}\n  Data: ${JSON.stringify(entry.data, null, 2)}`;
        }
        return message;
      })
      .join('\n');
  }

  downloadHistory(): void {
    const content = this.exportHistory();
    const blob = new Blob([content], { type: 'text/plain' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `wse-log-${Date.now()}.txt`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  }

  // ---------------------------------------------------------------------------
  // External Logging Service
  // ---------------------------------------------------------------------------

  private sendToLoggingService(entry: LogEntry): void {
    if (typeof localStorage === 'undefined') return;
    try {
      const existing = localStorage.getItem(ERROR_LOG_STORAGE_KEY);
      const parsed = existing ? JSON.parse(existing) : [];
      const logs: unknown[] = Array.isArray(parsed) ? parsed : [];

      // Deliberately no `data`/`error` fields: they may carry message payloads.
      logs.push({
        level: LogLevel[entry.level],
        timestamp: entry.timestamp.toISOString(),
        context: entry.context,
        message: entry.message,
      });

      if (logs.length > 100) {
        logs.splice(0, logs.length - 100);
      }

      localStorage.setItem(ERROR_LOG_STORAGE_KEY, JSON.stringify(logs));
    } catch {
      // Ignore storage errors
    }
  }

  // ---------------------------------------------------------------------------
  // Performance Monitoring
  // ---------------------------------------------------------------------------

  time(label: string): void {
    if (this.level <= LogLevel.DEBUG) {
      console.time(`[${this.context}] ${label}`);
    }
  }

  timeEnd(label: string): void {
    if (this.level <= LogLevel.DEBUG) {
      console.timeEnd(`[${this.context}] ${label}`);
    }
  }

  measure<T>(label: string, fn: () => T): T {
    if (this.level > LogLevel.DEBUG) {
      return fn();
    }

    const start = performance.now();
    try {
      const result = fn();
      const duration = performance.now() - start;
      this.debug(`${label} took ${duration.toFixed(2)}ms`);
      return result;
    } catch (error) {
      const duration = performance.now() - start;
      this.error(`${label} failed after ${duration.toFixed(2)}ms`, error as Error);
      throw error;
    }
  }

  async measureAsync<T>(label: string, fn: () => Promise<T>): Promise<T> {
    if (this.level > LogLevel.DEBUG) {
      return fn();
    }

    const start = performance.now();
    try {
      const result = await fn();
      const duration = performance.now() - start;
      this.debug(`${label} took ${duration.toFixed(2)}ms`);
      return result;
    } catch (error) {
      const duration = performance.now() - start;
      this.error(`${label} failed after ${duration.toFixed(2)}ms`, error as Error);
      throw error;
    }
  }
}

// Create default logger instance
export const logger = new Logger();

// Export for creating child loggers
export { Logger };
