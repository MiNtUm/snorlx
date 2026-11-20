import {
  createContext,
  useContext,
  useState,
  useCallback,
  useEffect,
  useRef,
  ReactNode,
} from 'react';
import { useSocket } from './SocketContext';
import { useQueryClient } from '@tanstack/react-query';
import { ApiError, api, type SyncStatus } from '../services/api';

interface SyncProgress {
  isSyncing: boolean;
  syncedCount: number;
  totalToSync: number;
  progress: number;
  currentRepo: string | null;
  /** Why the last sync stopped without finishing; cleared when the next one starts. */
  error: string | null;
}

interface SyncContextType {
  sync: SyncProgress;
  /** Starts a full sync and follows it until it ends. Resolves once the request was answered. */
  startSync: () => Promise<void>;
}

const IDLE: SyncProgress = {
  isSyncing: false,
  syncedCount: 0,
  totalToSync: 0,
  progress: 0,
  currentRepo: null,
  error: null,
};

/** How often the backend is asked for the sync state while a sync is shown. */
const STATUS_POLL_MS = 2000;
/** How long the finished state stays visible before the overlay resets. */
const RESET_DELAY_MS = 2000;
/** How long a failure message stays visible. */
const ERROR_DELAY_MS = 8000;

const SyncContext = createContext<SyncContextType | undefined>(undefined);

function percent(synced: number, total: number): number {
  return total > 0 ? Math.min(100, Math.round((synced / total) * 100)) : 0;
}

export function SyncProvider({ children }: { children: ReactNode }) {
  const [sync, setSync] = useState<SyncProgress>(IDLE);
  // Set once the backend answered the start request: from then on the status endpoint is
  // authoritative, so a lost WebSocket event can never leave the overlay stuck.
  const [following, setFollowing] = useState(false);
  const resetTimer = useRef<ReturnType<typeof setTimeout> | null>(null);

  const { subscribe } = useSocket();
  const queryClient = useQueryClient();

  const cancelReset = useCallback(() => {
    if (resetTimer.current) {
      clearTimeout(resetTimer.current);
      resetTimer.current = null;
    }
  }, []);

  useEffect(() => cancelReset, [cancelReset]);

  const refreshData = useCallback(() => {
    for (const key of ['repositories', 'workflows', 'runs', 'dashboard', 'pipelines']) {
      void queryClient.invalidateQueries({ queryKey: [key] });
    }
  }, [queryClient]);

  const resetLater = useCallback((ms: number) => {
    if (resetTimer.current) clearTimeout(resetTimer.current);
    resetTimer.current = setTimeout(() => {
      resetTimer.current = null;
      setSync(IDLE);
    }, ms);
  }, []);

  // The sync ended: reload the data and let the finished state show briefly before the overlay goes.
  const finish = useCallback(
    (error: string | null) => {
      setFollowing(false);
      cancelReset();
      if (error) {
        setSync(prev => ({ ...prev, isSyncing: false, error }));
        resetLater(ERROR_DELAY_MS);
        return;
      }
      setSync(prev => ({ ...prev, isSyncing: false, progress: 100 }));
      refreshData();
      resetLater(RESET_DELAY_MS);
    },
    [cancelReset, refreshData, resetLater],
  );

  const applyStatus = useCallback(
    (status: SyncStatus) => {
      if (status.running) {
        setSync(prev => ({
          ...prev,
          isSyncing: true,
          syncedCount: status.synced,
          totalToSync: status.total,
          progress: Math.max(prev.progress, percent(status.synced, status.total)),
          currentRepo: status.current || prev.currentRepo,
        }));
      } else {
        finish(status.error ?? null);
      }
    },
    [finish],
  );

  const startSync = useCallback(async () => {
    cancelReset();
    setSync({ ...IDLE, isSyncing: true });
    try {
      await api.syncRepositories();
    } catch (err) {
      // 409: a sync is already running (started in another tab, or in the background). Follow it.
      if (!(err instanceof ApiError && err.status === 409)) {
        setFollowing(false);
        setSync({ ...IDLE, error: 'Could not start the sync' });
        resetLater(ERROR_DELAY_MS);
        return;
      }
    }
    setFollowing(true);
  }, [cancelReset, resetLater]);

  // Events pushed over the WebSocket: delivered one by one, in order.
  useEffect(
    () =>
      subscribe(message => {
        const data = (message.data ?? {}) as Record<string, unknown>;
        switch (message.type) {
          case 'sync:start': {
            cancelReset();
            setSync({
              ...IDLE,
              isSyncing: true,
              totalToSync: (data.total as number) || 0,
            });
            break;
          }
          case 'sync:progress': {
            const synced = (data.synced as number) || 0;
            const total = (data.total as number) || 0;
            setSync(prev => ({
              ...prev,
              isSyncing: true,
              syncedCount: synced,
              totalToSync: total,
              progress: Math.max(prev.progress, percent(synced, total)),
              currentRepo: (data.current as string) || null,
            }));
            break;
          }
          case 'sync:complete':
            finish(null);
            break;
          case 'sync:error':
            finish((data.message as string) || 'Sync failed');
            break;
        }
      }),
    [subscribe, cancelReset, finish],
  );

  // Fallback while a sync is shown: ask the backend, in case a WebSocket event was lost (socket
  // down or reconnecting, tab in the background).
  useEffect(() => {
    if (!following) return;
    let cancelled = false;
    const poll = async () => {
      try {
        const status = await api.syncStatus();
        if (!cancelled) applyStatus(status);
      } catch {
        // Transient: the next tick asks again
      }
    };
    void poll();
    const id = setInterval(() => void poll(), STATUS_POLL_MS);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, [following, applyStatus]);

  // A page loaded while a manual sync runs picks its progress up again.
  useEffect(() => {
    let cancelled = false;
    api
      .syncStatus()
      .then(status => {
        if (cancelled || !status.running || !status.manual) return;
        setSync(prev => (prev.isSyncing ? prev : { ...IDLE, isSyncing: true }));
        setFollowing(true);
      })
      .catch(() => {
        // Not signed in or backend unreachable: nothing to resume
      });
    return () => {
      cancelled = true;
    };
  }, []);

  return <SyncContext.Provider value={{ sync, startSync }}>{children}</SyncContext.Provider>;
}

export function useSync() {
  const context = useContext(SyncContext);
  if (!context) {
    throw new Error('useSync must be used within SyncProvider');
  }
  return context;
}
