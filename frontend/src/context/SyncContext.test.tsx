import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import { act, renderHook } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import type { ReactNode } from "react";
import { SyncProvider, useSync } from "./SyncContext";
import { ApiError, type SyncStatus } from "../services/api";
import type { LiveMessage } from "../lib/liveUpdates";

const listeners = new Set<(message: LiveMessage) => void>();
const mockSync = vi.fn();
const mockStatus = vi.fn();

vi.mock("./SocketContext", () => ({
	useSocket: () => ({
		isConnected: true,
		lastMessage: null,
		subscribe: (listener: (message: LiveMessage) => void) => {
			listeners.add(listener);
			return () => listeners.delete(listener);
		},
	}),
}));

vi.mock("../services/api", async (importOriginal) => {
	const actual = await importOriginal<typeof import("../services/api")>();
	return {
		...actual,
		api: {
			syncRepositories: () => mockSync(),
			syncStatus: () => mockStatus(),
		},
	};
});

function push(message: LiveMessage) {
	for (const listener of listeners) listener(message);
}

const idle: SyncStatus = { running: false, manual: false, total: 0, synced: 0, current: "" };

describe("SyncProvider", () => {
	let queryClient: QueryClient;
	let invalidate: ReturnType<typeof vi.spyOn>;

	const wrapper = ({ children }: { children: ReactNode }) => (
		<QueryClientProvider client={queryClient}>
			<SyncProvider>{children}</SyncProvider>
		</QueryClientProvider>
	);

	beforeEach(() => {
		vi.useFakeTimers();
		listeners.clear();
		queryClient = new QueryClient();
		invalidate = vi.spyOn(queryClient, "invalidateQueries");
		mockSync.mockReset().mockResolvedValue({ status: "started" });
		mockStatus.mockReset().mockResolvedValue(idle);
	});

	afterEach(() => {
		vi.useRealTimers();
	});

	async function mounted() {
		const hook = renderHook(() => useSync(), { wrapper });
		await act(async () => {});
		return hook;
	}

	it("keeps every event of a burst, in order", async () => {
		const { result } = await mounted();

		await act(async () => {
			void result.current.startSync();
		});
		act(() => {
			// All delivered within one tick: a single "last message" slot would keep only the last.
			push({ type: "sync:start", data: { total: 4 } });
			push({ type: "sync:progress", data: { synced: 1, total: 4, current: "acme/a" } });
			push({ type: "sync:progress", data: { synced: 2, total: 4, current: "acme/b" } });
		});

		expect(result.current.sync).toMatchObject({
			isSyncing: true,
			syncedCount: 2,
			totalToSync: 4,
			progress: 50,
			currentRepo: "acme/b",
		});
	});

	it("never moves the bar backwards", async () => {
		const { result } = await mounted();
		act(() => {
			push({ type: "sync:start", data: { total: 4 } });
			push({ type: "sync:progress", data: { synced: 3, total: 4, current: "acme/c" } });
			push({ type: "sync:progress", data: { synced: 1, total: 4, current: "acme/a" } });
		});
		expect(result.current.sync.progress).toBe(75);
	});

	it("finishes on sync:complete, reloads data and resets after a moment", async () => {
		const { result } = await mounted();
		act(() => {
			push({ type: "sync:start", data: { total: 2 } });
			push({ type: "sync:complete", data: {} });
		});

		expect(result.current.sync).toMatchObject({ isSyncing: false, progress: 100 });
		expect(invalidate).toHaveBeenCalledWith({ queryKey: ["repositories"] });
		expect(invalidate).toHaveBeenCalledWith({ queryKey: ["dashboard"] });

		act(() => {
			vi.advanceTimersByTime(2000);
		});
		expect(result.current.sync).toMatchObject({ isSyncing: false, progress: 0, totalToSync: 0 });
	});

	it("does not wipe a sync that starts while the previous one is still showing 100%", async () => {
		const { result } = await mounted();
		act(() => {
			push({ type: "sync:complete", data: {} });
		});
		act(() => {
			vi.advanceTimersByTime(1000);
			push({ type: "sync:start", data: { total: 3 } });
		});
		act(() => {
			vi.advanceTimersByTime(1500);
		});
		expect(result.current.sync).toMatchObject({ isSyncing: true, totalToSync: 3 });
	});

	it("reports a failure instead of hanging when the request is rejected", async () => {
		mockSync.mockRejectedValue(new ApiError("Unauthorized", 401));
		const { result } = await mounted();

		await act(async () => {
			await result.current.startSync();
		});

		expect(result.current.sync).toMatchObject({ isSyncing: false, error: "Could not start the sync" });
		act(() => {
			vi.advanceTimersByTime(8000);
		});
		expect(result.current.sync.error).toBeNull();
	});

	it("shows the error of a failed sync", async () => {
		const { result } = await mounted();
		act(() => {
			push({ type: "sync:start", data: { total: 2 } });
			push({ type: "sync:error", data: { message: "Failed to fetch repositories from GitHub" } });
		});
		expect(result.current.sync).toMatchObject({
			isSyncing: false,
			error: "Failed to fetch repositories from GitHub",
		});
	});

	it("recovers from lost WebSocket events through the status endpoint", async () => {
		const { result } = await mounted();
		mockStatus.mockResolvedValue({ running: true, manual: true, total: 10, synced: 5, current: "acme/e" });

		await act(async () => {
			await result.current.startSync();
		});
		expect(result.current.sync).toMatchObject({ isSyncing: true, syncedCount: 5, progress: 50, currentRepo: "acme/e" });

		// The sync ends but no sync:complete ever arrives.
		mockStatus.mockResolvedValue({ ...idle, total: 10, synced: 10 });
		await act(async () => {
			await vi.advanceTimersByTimeAsync(2000);
		});
		expect(result.current.sync).toMatchObject({ isSyncing: false, progress: 100 });
		expect(invalidate).toHaveBeenCalledWith({ queryKey: ["repositories"] });
	});

	it("follows a sync that is already running when the request answers 409", async () => {
		mockSync.mockRejectedValue(new ApiError('{"status":"already_running"}', 409));
		const { result } = await mounted();
		mockStatus.mockResolvedValue({ running: true, manual: false, total: 8, synced: 2, current: "acme/b" });

		await act(async () => {
			await result.current.startSync();
		});
		expect(result.current.sync).toMatchObject({ isSyncing: true, syncedCount: 2, error: null });

		mockStatus.mockResolvedValue(idle);
		await act(async () => {
			await vi.advanceTimersByTimeAsync(2000);
		});
		expect(result.current.sync.isSyncing).toBe(false);
	});

	it("resumes the progress of a manual sync after a page reload", async () => {
		mockStatus.mockResolvedValue({ running: true, manual: true, total: 6, synced: 3, current: "acme/c" });
		const { result } = await mounted();
		expect(result.current.sync).toMatchObject({ isSyncing: true, syncedCount: 3, progress: 50 });
	});

	it("stays quiet for a background sync it did not start", async () => {
		mockStatus.mockResolvedValue({ running: true, manual: false, total: 6, synced: 3, current: "acme/c" });
		const { result } = await mounted();
		expect(result.current.sync.isSyncing).toBe(false);
	});
});
