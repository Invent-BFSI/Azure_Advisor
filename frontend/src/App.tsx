import { useCallback, useEffect, useRef, useState } from "react";
import ReactMarkdown from "react-markdown";

type LogEntry = { id: string; text: string };

type WsEvent = {
    type: string;
    delta?: string;
    transcript?: string;
    item_id?: string;
    status?: string;
    payload?: unknown;
    session_id?: string;
    name?: string;
};

const BACKEND_HTTP_BASE =
    (import.meta.env.VITE_BACKEND_BASE as string | undefined) ??
    window.location.origin;
const BACKEND_WS_BASE = BACKEND_HTTP_BASE.replace(/^http/, "ws");
const TARGET_SAMPLE_RATE = 24000;
const INT16_MAX = 32767;

/* ── audio helpers (unchanged) ─────────────────────── */

function float32ToBase64(data: Float32Array): string {
    const buffer = new Uint8Array(data.buffer);
    let result = "";
    for (let i = 0; i < buffer.length; i += 1) {
        result += String.fromCharCode(buffer[i]);
    }
    return btoa(result);
}

function downsampleBuffer(
    buffer: Float32Array,
    inputRate: number,
    targetRate: number
): Float32Array {
    if (targetRate === inputRate) return buffer;
    const ratio = inputRate / targetRate;
    const newLength = Math.round(buffer.length / ratio);
    const result = new Float32Array(newLength);
    let offsetResult = 0;
    let offsetBuffer = 0;
    while (offsetResult < result.length) {
        const nextOffsetBuffer = Math.round((offsetResult + 1) * ratio);
        let accum = 0;
        let count = 0;
        for (
            let i = offsetBuffer;
            i < nextOffsetBuffer && i < buffer.length;
            i += 1
        ) {
            accum += buffer[i];
            count += 1;
        }
        result[offsetResult] = count > 0 ? accum / count : 0;
        offsetResult += 1;
        offsetBuffer = nextOffsetBuffer;
    }
    return result;
}

function pcm16Base64ToFloat32(b64: string): Float32Array<ArrayBuffer> {
    const binary = atob(b64);
    const len = binary.length / 2;
    const result = new Float32Array(len) as Float32Array<ArrayBuffer>;
    for (let i = 0; i < len; i += 1) {
        const index = i * 2;
        const sample =
            (binary.charCodeAt(index + 1) << 8) | binary.charCodeAt(index);
        const signed = sample >= 0x8000 ? sample - 0x10000 : sample;
        result[i] = signed / INT16_MAX;
    }
    return result;
}

function useLog(): [LogEntry[], (message: string) => void] {
    const [entries, setEntries] = useState<LogEntry[]>([]);
    const append = useCallback((text: string) => {
        setEntries((prev: LogEntry[]) => [
            { id: crypto.randomUUID(), text },
            ...prev.slice(0, 99),
        ]);
    }, []);
    return [entries, append];
}

/* ── inline SVG icons (13x13, fill=currentColor) ──── */

const IconRefresh = () => (
    <svg viewBox="0 0 16 16" width="13" height="13" fill="currentColor">
        <path d="M13.65 2.35A7.96 7.96 0 0 0 8 0a8 8 0 1 0 8 8h-2a6 6 0 1 1-1.76-4.24L9 7h7V0l-2.35 2.35z" />
    </svg>
);

const IconMic = () => (
    <svg viewBox="0 0 16 16" width="13" height="13" fill="currentColor">
        <path d="M8 10a2.5 2.5 0 0 0 2.5-2.5v-4a2.5 2.5 0 0 0-5 0v4A2.5 2.5 0 0 0 8 10zm4-2.5a4 4 0 0 1-3.25 3.93V13.5h2a.75.75 0 0 1 0 1.5h-5.5a.75.75 0 0 1 0-1.5h2v-2.07A4 4 0 0 1 4 7.5a.75.75 0 0 1 1.5 0 2.5 2.5 0 0 0 5 0 .75.75 0 0 1 1.5 0z" />
    </svg>
);

const IconMicOff = () => (
    <svg viewBox="0 0 16 16" width="13" height="13" fill="currentColor">
        <path d="M12.73 12.02L2.98 2.27a.75.75 0 0 0-1.06 1.06l2.58 2.58v1.59a3.5 3.5 0 0 0 3.25 3.49V13H5.5a.75.75 0 0 0 0 1.5h5a.75.75 0 0 0 0-1.5H8.25v-2.01a3.5 3.5 0 0 0 3-2.6l2.54 2.54a.75.75 0 0 0 1.06-1.06l-.12-.12zM5 7.5v-1l4.55 4.55A3.5 3.5 0 0 1 5 7.5zM10.5 4v3.5c0 .17-.01.34-.04.5l1.13 1.13A5 5 0 0 0 12 7.5a.75.75 0 0 1 1.5 0 6.5 6.5 0 0 1-.67 2.88l1.07 1.07A7.97 7.97 0 0 0 5.5 4V3.5a2.5 2.5 0 0 1 5 0v.5z" />
    </svg>
);

const IconPlay = () => (
    <svg viewBox="0 0 16 16" width="13" height="13" fill="currentColor">
        <path d="M4 2.5v11a.5.5 0 0 0 .77.42l9-5.5a.5.5 0 0 0 0-.84l-9-5.5A.5.5 0 0 0 4 2.5z" />
    </svg>
);

const IconPause = () => (
    <svg viewBox="0 0 16 16" width="13" height="13" fill="currentColor">
        <path d="M3.5 2h3a.5.5 0 0 1 .5.5v11a.5.5 0 0 1-.5.5h-3a.5.5 0 0 1-.5-.5v-11a.5.5 0 0 1 .5-.5zm6 0h3a.5.5 0 0 1 .5.5v11a.5.5 0 0 1-.5.5h-3a.5.5 0 0 1-.5-.5v-11a.5.5 0 0 1 .5-.5z" />
    </svg>
);

const IconStop = () => (
    <svg viewBox="0 0 16 16" width="13" height="13" fill="currentColor">
        <rect x="3" y="3" width="10" height="10" rx="1" />
    </svg>
);

const IconSend = () => (
    <svg viewBox="0 0 16 16" width="13" height="13" fill="currentColor">
        <path d="M1.5 1.3a.5.5 0 0 1 .7-.2l12.5 6.5a.5.5 0 0 1 0 .88L2.2 14.93a.5.5 0 0 1-.7-.43V9.75L9 8 1.5 6.25V1.3z" />
    </svg>
);

/* ── App ───────────────────────────────────────────── */

function App() {
    useEffect(() => {
        document.title = "Aria - AI Investment Advisor";
    }, []);

    const [sessionId, setSessionId] = useState<string | null>(null);
    const [micActive, setMicActive] = useState(false);
    const [avatarReady, setAvatarReady] = useState(false);
    const [avatarLoading, setAvatarLoading] = useState(false);
    const [avatarPaused, setAvatarPaused] = useState(false);
    const [assistantTranscript, setAssistantTranscript] = useState("");
    const [userTranscript, setUserTranscript] = useState("");
    const [textInput, setTextInput] = useState("");
    const [, appendLog] = useLog();
    const [avatarIceServers, setAvatarIceServers] = useState<RTCIceServer[]>(
        []
    );

    const wsRef = useRef<WebSocket | null>(null);
    const pcRef = useRef<RTCPeerConnection | null>(null);
    const videoRef = useRef<HTMLVideoElement | null>(null);
    const remoteAudioRef = useRef<HTMLAudioElement | null>(null);

    const mediaStreamRef = useRef<MediaStream | null>(null);
    const audioCtxRef = useRef<AudioContext | null>(null);
    const processorRef = useRef<ScriptProcessorNode | null>(null);

    const playbackCtxRef = useRef<AudioContext | null>(null);
    const playbackCursorRef = useRef<number>(0);

    const ensurePlaybackContext = useCallback(() => {
        if (!playbackCtxRef.current) {
            playbackCtxRef.current = new AudioContext({
                sampleRate: TARGET_SAMPLE_RATE,
            });
            playbackCursorRef.current =
                playbackCtxRef.current.currentTime;
        }
        const ctx = playbackCtxRef.current;
        if (ctx?.state === "suspended") {
            ctx.resume().catch(() => undefined);
        }
        return playbackCtxRef.current;
    }, []);

    const schedulePlayback = useCallback(
        (deltaB64: string) => {
            const audioCtx = ensurePlaybackContext();
            const floatSamples = pcm16Base64ToFloat32(deltaB64);
            if (!floatSamples.length) return;
            const buffer = audioCtx.createBuffer(
                1,
                floatSamples.length,
                TARGET_SAMPLE_RATE
            );
            buffer.copyToChannel(floatSamples, 0);
            const source = audioCtx.createBufferSource();
            source.buffer = buffer;
            source.connect(audioCtx.destination);
            const startAt = Math.max(
                playbackCursorRef.current,
                audioCtx.currentTime + 0.02
            );
            source.start(startAt);
            playbackCursorRef.current = startAt + buffer.duration;
        },
        [ensurePlaybackContext]
    );

    const teardownMic = useCallback(() => {
        processorRef.current?.disconnect();
        audioCtxRef.current?.close().catch(() => undefined);
        mediaStreamRef.current
            ?.getTracks()
            .forEach((track: MediaStreamTrack) => track.stop());
        processorRef.current = null;
        audioCtxRef.current = null;
        mediaStreamRef.current = null;
        setMicActive(false);
    }, []);

    useEffect(() => () => teardownMic(), [teardownMic]);

    /* ── WebSocket ──────────────────────────────────── */

    const connectWebSocket = useCallback(
        (id: string) => {
            const ws = new WebSocket(
                `${BACKEND_WS_BASE}/ws/sessions/${id}`
            );
            wsRef.current = ws;

            ws.onopen = () => appendLog("WebSocket connected");
            ws.onclose = () => {
                appendLog("WebSocket closed");
                teardownMic();
            };
            ws.onerror = (event: Event) =>
                appendLog(`WebSocket error: ${event.type}`);

            ws.onmessage = (msg) => {
                const data: WsEvent = JSON.parse(msg.data);
                switch (data.type) {
                    case "session_ready":
                        if (data.session_id) {
                            appendLog(`Session ready: ${data.session_id}`);
                        }
                        break;
                    case "assistant_audio_delta":
                        if (typeof data.delta === "string") {
                            schedulePlayback(data.delta);
                        }
                        break;
                    case "assistant_transcript_delta":
                        if (typeof data.delta === "string") {
                            setAssistantTranscript(
                                (prev: string) => prev + data.delta
                            );
                        }
                        break;
                    case "assistant_transcript_done":
                        if (typeof data.transcript === "string") {
                            setAssistantTranscript(data.transcript);
                        }
                        break;
                    case "user_transcript_completed":
                        if (typeof data.transcript === "string") {
                            setUserTranscript(data.transcript);
                        }
                        break;
                    case "function_call_completed":
                        appendLog(
                            `Function call completed: ${data.name ?? "unknown"}`
                        );
                        break;
                    case "error":
                        appendLog(
                            `Server error: ${JSON.stringify(data.payload)}`
                        );
                        break;
                    case "event": {
                        const payload = data.payload as
                            | Record<string, any>
                            | undefined;
                        if (payload?.type === "session.updated") {
                            const session = payload.session ?? {};
                            const avatar = session.avatar ?? {};
                            const candidateSources = [
                                avatar.ice_servers,
                                session.rtc?.ice_servers,
                                session.ice_servers,
                            ].find((value) => Array.isArray(value));
                            if (candidateSources) {
                                const normalized: RTCIceServer[] =
                                    candidateSources
                                        .map((entry: any) => {
                                            if (typeof entry === "string") {
                                                return {
                                                    urls: entry,
                                                } as RTCIceServer;
                                            }
                                            if (
                                                entry &&
                                                typeof entry === "object"
                                            ) {
                                                const {
                                                    urls,
                                                    username,
                                                    credential,
                                                } = entry;
                                                if (!urls) return null;
                                                return {
                                                    urls,
                                                    username,
                                                    credential,
                                                } as RTCIceServer;
                                            }
                                            return null;
                                        })
                                        .filter(
                                            (
                                                entry
                                            ): entry is RTCIceServer =>
                                                Boolean(entry)
                                        );
                                if (normalized.length) {
                                    setAvatarIceServers(normalized);
                                    appendLog(
                                        `Received ${normalized.length} ICE server${normalized.length > 1 ? "s" : ""} from session`
                                    );
                                }
                            }
                        }
                        break;
                    }
                    default:
                        break;
                }
            };
        },
        [appendLog, schedulePlayback, teardownMic]
    );

    const createSession = useCallback(async () => {
        const response = await fetch(`${BACKEND_HTTP_BASE}/sessions`, {
            method: "POST",
        });
        if (!response.ok) {
            throw new Error(
                `Failed to create session: ${response.status}`
            );
        }
        const { session_id } = await response.json();
        setSessionId(session_id);
        appendLog(`Session created: ${session_id}`);
        connectWebSocket(session_id);
        return session_id;
    }, [appendLog, connectWebSocket]);

    useEffect(() => {
        createSession().catch((err: unknown) =>
            appendLog(`Error creating session: ${String(err)}`)
        );
    }, [appendLog, createSession]);

    /* ── Microphone ─────────────────────────────────── */

    const startMic = useCallback(async () => {
        if (!wsRef.current) {
            appendLog("WebSocket not ready");
            return;
        }
        const mediaStream = await navigator.mediaDevices.getUserMedia({
            audio: true,
        });
        const audioContext = new AudioContext();
        if (audioContext.state === "suspended") {
            try {
                await audioContext.resume();
            } catch {
                /* ignore */
            }
        }

        const playbackCtx = ensurePlaybackContext();
        if (playbackCtx && playbackCtx.state === "suspended") {
            try {
                await playbackCtx.resume();
            } catch {
                /* ignore */
            }
        }

        const source = audioContext.createMediaStreamSource(mediaStream);
        const processor = audioContext.createScriptProcessor(4096, 1, 1);
        processor.onaudioprocess = (event: AudioProcessingEvent) => {
            const input = event.inputBuffer.getChannelData(0);
            const downsampled = downsampleBuffer(
                input,
                audioContext.sampleRate,
                TARGET_SAMPLE_RATE
            );
            if (!downsampled.length) return;
            const base64 = float32ToBase64(downsampled);
            wsRef.current?.send(
                JSON.stringify({
                    type: "audio_chunk",
                    data: base64,
                    encoding: "float32",
                })
            );
        };
        source.connect(processor);
        processor.connect(audioContext.destination);

        mediaStreamRef.current = mediaStream;
        audioCtxRef.current = audioContext;
        processorRef.current = processor;
        setMicActive(true);
        appendLog("Microphone streaming started");
    }, [appendLog, ensurePlaybackContext]);

    const stopMic = useCallback(() => {
        teardownMic();
        appendLog("Microphone streaming stopped");
    }, [appendLog, teardownMic]);

    /* ── Text prompt ────────────────────────────────── */

    const sendTextPrompt = useCallback(async () => {
        if (!sessionId || !textInput.trim()) return;
        const text = textInput.trim();
        setTextInput("");
        const response = await fetch(
            `${BACKEND_HTTP_BASE}/sessions/${sessionId}/text`,
            {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ text }),
            }
        );
        if (!response.ok) {
            appendLog(`Failed to send text: ${response.status}`);
        }
    }, [appendLog, sessionId, textInput]);

    const clearTranscripts = useCallback(() => {
        setUserTranscript("");
        setAssistantTranscript("");
    }, []);

    /* ── Avatar ─────────────────────────────────────── */

    const startAvatar = useCallback(async () => {
        if (!sessionId) {
            appendLog("Session not ready");
            return;
        }
        if (pcRef.current) {
            appendLog("Avatar already connected");
            return;
        }

        setAvatarLoading(true);
        appendLog(
            `Initializing avatar connection... (ICE servers: ${avatarIceServers.length})`
        );

        try {
            const pc = new RTCPeerConnection({
                bundlePolicy: "max-bundle",
                iceServers: avatarIceServers,
            });
            pcRef.current = pc;

            pc.oniceconnectionstatechange = () => {
                appendLog(
                    `ICE connection state: ${pc.iceConnectionState}`
                );
            };
            pc.onconnectionstatechange = () => {
                appendLog(
                    `Peer connection state: ${pc.connectionState}`
                );
            };
            pc.onicecandidateerror = (event: any) => {
                appendLog(
                    `ICE candidate error: ${event.errorCode} ${event.errorText || ""} ${event.url || ""}`
                );
            };

            pc.addTransceiver("audio", { direction: "recvonly" });
            pc.addTransceiver("video", { direction: "recvonly" });

            pc.ontrack = (event) => {
                const [stream] = event.streams;
                if (!stream) return;

                if (event.track.kind === "video" && videoRef.current) {
                    videoRef.current.srcObject = stream;
                    videoRef.current.play().catch(() => {
                        /* ignore auto-play rejection */
                    });
                    appendLog("Avatar video track received");
                }

                if (event.track.kind === "audio") {
                    let audioEl = remoteAudioRef.current;
                    if (!audioEl) {
                        audioEl = document.createElement("audio");
                        audioEl.autoplay = true;
                        audioEl.controls = false;
                        audioEl.style.display = "none";
                        audioEl.setAttribute("playsinline", "true");
                        audioEl.muted = false;
                        document.body.appendChild(audioEl);
                        remoteAudioRef.current = audioEl;
                    }
                    audioEl.srcObject = stream;
                    audioEl.play().catch(() => undefined);
                    appendLog("Avatar audio track received");
                }
            };

            const gatheringFinished = new Promise<void>((resolve) => {
                if (pc.iceGatheringState === "complete") {
                    resolve();
                } else {
                    pc.addEventListener("icegatheringstatechange", () => {
                        if (pc.iceGatheringState === "complete") {
                            resolve();
                        }
                    });
                }
            });

            const offer = await pc.createOffer();
            await pc.setLocalDescription(offer);
            await gatheringFinished;

            const localSdp = pc.localDescription?.sdp;
            if (!localSdp) {
                appendLog("Failed to obtain local SDP");
                return;
            }

            const response = await fetch(
                `${BACKEND_HTTP_BASE}/sessions/${sessionId}/avatar-offer`,
                {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({ sdp: localSdp }),
                }
            );

            if (!response.ok) {
                appendLog(`Avatar offer failed: ${response.status}`);
                setAvatarLoading(false);
                return;
            }

            const { sdp } = await response.json();
            await pc.setRemoteDescription({ type: "answer", sdp });
            setAvatarLoading(false);
            setAvatarReady(true);
            appendLog("Avatar connected");
        } catch (error) {
            appendLog(`Avatar connection error: ${String(error)}`);
            setAvatarLoading(false);
            if (pcRef.current) {
                pcRef.current.close();
                pcRef.current = null;
            }
        }
    }, [appendLog, sessionId, avatarIceServers]);

    const teardownAvatar = useCallback(() => {
        pcRef.current?.close();
        pcRef.current = null;
        if (videoRef.current) {
            videoRef.current.srcObject = null;
        }
        if (remoteAudioRef.current) {
            remoteAudioRef.current.pause();
            remoteAudioRef.current.srcObject = null;
            remoteAudioRef.current.remove();
            remoteAudioRef.current = null;
        }
        setAvatarLoading(false);
        setAvatarReady(false);
        setAvatarPaused(false);
        appendLog("Avatar connection closed");
    }, [appendLog]);

    const pauseAvatar = useCallback(() => {
        if (videoRef.current) videoRef.current.pause();
        if (remoteAudioRef.current) remoteAudioRef.current.pause();
        setAvatarPaused(true);
        appendLog("Avatar paused");
    }, [appendLog]);

    const unpauseAvatar = useCallback(() => {
        if (videoRef.current) {
            videoRef.current.play().catch(() => {});
        }
        if (remoteAudioRef.current) {
            remoteAudioRef.current.play().catch(() => {});
        }
        setAvatarPaused(false);
        appendLog("Avatar resumed");
    }, [appendLog]);

    /* ── Render ─────────────────────────────────────── */

    return (
        <main>
            {/* ── Header ─────────────────────────────── */}
            <header className="page-header">
                <div className="header-left">
                    <span className="live-badge">
                        <span className="pulse-dot" />
                        Live Session
                    </span>
                    <h1 className="aria-title">Aria</h1>
                    <p className="subtitle">
                        Your personalized investment profile assistant
                        powered by Azure Voice Live
                    </p>
                </div>
                {sessionId && (
                    <span className="session-tag">
                        Session {sessionId.slice(0, 8)}
                    </span>
                )}
            </header>

            {/* ── Controls card ──────────────────────── */}
            <div className="card">
                <div className="card-header">
                    <span className="card-title">Controls</span>
                </div>
                <div className="card-body">
                    <div className="controls-row">
                        <button
                            className="btn btn-refresh"
                            onClick={() => window.location.reload()}
                            title="Refresh session"
                        >
                            <IconRefresh />
                            Refresh
                        </button>

                        <div className="ctrl-divider" />

                        <button
                            className={`btn ${micActive ? "btn-mic-active" : "btn-mic"}`}
                            onClick={micActive ? stopMic : startMic}
                        >
                            {micActive ? <IconMicOff /> : <IconMic />}
                            {micActive ? "Stop Mic" : "Start Microphone"}
                        </button>

                        <div className="ctrl-divider" />

                        <button
                            className="btn btn-start-avatar"
                            onClick={startAvatar}
                            disabled={
                                !sessionId ||
                                avatarLoading ||
                                avatarReady
                            }
                        >
                            <IconPlay />
                            {avatarLoading
                                ? "Connecting..."
                                : "Start Avatar"}
                        </button>

                        <button
                            className="btn btn-pause"
                            onClick={
                                avatarPaused ? unpauseAvatar : pauseAvatar
                            }
                            disabled={!avatarReady || avatarLoading}
                        >
                            <IconPause />
                            {avatarPaused ? "Resume" : "Pause Avatar"}
                        </button>

                        <button
                            className="btn btn-stop"
                            onClick={teardownAvatar}
                            disabled={!avatarReady && !avatarLoading}
                        >
                            <IconStop />
                            Stop Avatar
                        </button>
                    </div>
                </div>
            </div>

            {/* ── Side-by-side: Avatar + Transcripts ── */}
            <div className="split-row">

            {/* ── Avatar Stream card ─────────────────── */}
            <div className="card card-avatar">
                <div className="card-header">
                    <span className="card-title">Avatar Stream</span>
                    {avatarReady && (
                        <span className="connected-pill">
                            <span className="pulse-dot" />
                            Connected
                        </span>
                    )}
                </div>
                <div className="avatar-body">
                    <div className="video-container">
                        <video
                            ref={videoRef}
                            autoPlay
                            playsInline
                            muted={false}
                            controls={false}
                        />

                        {/* Lower-third */}
                        {avatarReady && !avatarPaused && (
                            <div className="lower-third">
                                <div>
                                    <div className="lower-third-name">
                                        Aria
                                    </div>
                                    <div className="lower-third-role">
                                        AI Investment Advisor
                                    </div>
                                </div>
                            </div>
                        )}

                        {/* Overlays */}
                        {avatarLoading && (
                            <div className="avatar-loading-overlay">
                                <div className="loading-spinner" />
                                <p>Loading Avatar...</p>
                            </div>
                        )}
                        {avatarPaused && avatarReady && (
                            <div className="avatar-paused-overlay">
                                <div className="pause-icon">
                                    <IconPause />
                                </div>
                                <p>Avatar Paused</p>
                            </div>
                        )}
                        {!avatarReady && !avatarLoading && (
                            <div className="avatar-placeholder">
                                <p>
                                    Click "Start Avatar" to begin video
                                    stream
                                </p>
                            </div>
                        )}
                    </div>
                </div>
            </div>

            {/* ── Transcripts card ───────────────────── */}
            <div className="card card-transcript">
                <div className="card-header">
                    <span className="card-title">Transcripts</span>
                    <button
                        className="btn-clear"
                        onClick={clearTranscripts}
                    >
                        Clear
                    </button>
                </div>
                <div className="card-body">
                    {/* User row */}
                    <div className="transcript-row">
                        <div className="avatar-circle avatar-circle-user">
                            YOU
                        </div>
                        <div className="transcript-content">
                            <div className="role-label role-label-user">
                                You
                            </div>
                            <div
                                className={`message-bubble${!userTranscript ? " placeholder" : ""}`}
                            >
                                {userTranscript ||
                                    "(waiting for speech\u2026)"}
                            </div>
                        </div>
                    </div>

                    <div className="transcript-divider" />

                    {/* Assistant row */}
                    <div className="transcript-row">
                        <div className="avatar-circle avatar-circle-ai">
                            AI
                        </div>
                        <div className="transcript-content">
                            <div className="role-label role-label-ai">
                                Aria
                            </div>
                            <div
                                className={`message-bubble${!assistantTranscript ? " placeholder" : ""}`}
                            >
                                {assistantTranscript ? (
                                    <div className="assistant-response">
                                        <ReactMarkdown>
                                            {assistantTranscript}
                                        </ReactMarkdown>
                                    </div>
                                ) : (
                                    "(waiting for response\u2026)"
                                )}
                            </div>
                        </div>
                    </div>
                </div>

                {/* Text prompt bar */}
                <div className="prompt-bar">
                    <input
                        className="prompt-input"
                        type="text"
                        placeholder="Type a message..."
                        value={textInput}
                        onChange={(e) => setTextInput(e.target.value)}
                        onKeyDown={(e) => {
                            if (e.key === "Enter") sendTextPrompt();
                        }}
                    />
                    <button
                        className="btn-send"
                        onClick={sendTextPrompt}
                        disabled={!sessionId || !textInput.trim()}
                    >
                        <IconSend />
                        Send
                    </button>
                </div>
            </div>

            </div>{/* end split-row */}
        </main>
    );
}

export default App;
