/**
 * DiscordManager – wraps a selfbot Client + Streamer for voice & Go Live.
 *
 * Uses discord.js-selfbot-v13 for voice receiving (per-user audio) and
 * @dank074/discord-video-stream for Go Live streaming.
 */

import { Client } from "discord.js-selfbot-v13";
import type {
    VoiceChannel,
    StageChannel,
    VoiceConnection,
} from "discord.js-selfbot-v13";
import {
    Streamer,
    prepareStream,
    playStream,
    Encoders,
    Utils,
} from "@dank074/discord-video-stream";
import { Readable, PassThrough } from "node:stream";
import { spawn, type ChildProcess } from "node:child_process";

// ─── Types ───────────────────────────────────────────────────────────────────

export interface VoiceUser {
    id: string;
    username: string;
    displayName: string;
    avatar: string;
    speaking: boolean;
    volume: number; // 0.0–2.0 gain multiplier
}

export interface StreamOptions {
    height?: number;
    fps?: number;
    bitrate?: number;
    bitrateMax?: number;
    codec?: string;
}

interface BotUser {
    id: string;
    tag: string;
    avatar: string;
}

/** Per-user audio state */
interface UserAudio {
    stream: Readable | null;
    volume: number;
    subscribers: Set<PassThrough>;
    /** Track bytes received for debugging */
    bytesReceived: number;
    /** Timestamp of last audio data received (ms) */
    lastDataTime: number;
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

/** Apply 16-bit LE stereo volume scaling in-place */
function applyVolume(buf: Buffer, gain: number): Buffer {
    if (gain === 1.0) return buf;
    const out = Buffer.alloc(buf.length);
    for (let i = 0; i < buf.length; i += 2) {
        let sample = buf.readInt16LE(i);
        sample = Math.max(-32768, Math.min(32767, Math.round(sample * gain)));
        out.writeInt16LE(sample, i);
    }
    return out;
}

/** 48 kHz stereo 16-bit WAV header for streaming */
function wavHeader(): Buffer {
    const buf = Buffer.alloc(44);
    buf.write("RIFF", 0);
    buf.writeUInt32LE(0xffffffff, 4); // unknown length
    buf.write("WAVE", 8);
    buf.write("fmt ", 12);
    buf.writeUInt32LE(16, 16);         // chunk size
    buf.writeUInt16LE(1, 20);          // PCM
    buf.writeUInt16LE(2, 22);          // stereo
    buf.writeUInt32LE(48000, 24);      // sample rate
    buf.writeUInt32LE(48000 * 2 * 2, 28); // byte rate
    buf.writeUInt16LE(4, 32);          // block align
    buf.writeUInt16LE(16, 34);         // bits per sample
    buf.write("data", 36);
    buf.writeUInt32LE(0xffffffff, 40); // unknown length
    return buf;
}

// ─── Manager ─────────────────────────────────────────────────────────────────

export class DiscordManager {
    private client: Client;
    private streamer: Streamer;
    private _connected = false;
    private _guildId?: string;
    private _channelId?: string;
    private _streaming = false;
    private _streamAbort?: AbortController;
    private _voiceUsers = new Map<string, VoiceUser>();
    private _voiceConn?: VoiceConnection;
    private _userAudio = new Map<string, UserAudio>();
    private _audioDebugLogged = false;
    private _silenceTimer?: ReturnType<typeof setInterval>;
    private readonly _silenceBuf = Buffer.alloc(3840); // 20ms of 48kHz stereo 16-bit silence
    private _ingestProcess?: ChildProcess;

    constructor() {
        this.client = new Client({ checkUpdate: false } as any);
        this.streamer = new Streamer(this.client);
    }

    // ── Connection ───────────────────────────────────────────────────────────

    async connect(token: string): Promise<void> {
        if (this._connected) return;
        return new Promise<void>((resolve, reject) => {
            const timeout = setTimeout(() => reject(new Error("Login timeout")), 30_000);
            this.client.once("ready", () => {
                clearTimeout(timeout);
                this._connected = true;
                this._setupListeners();
                resolve();
            });
            this.client.login(token).catch((err) => {
                clearTimeout(timeout);
                reject(err);
            });
        });
    }

    async disconnect(): Promise<void> {
        if (this._streaming) await this.stopStream();
        if (this._channelId) await this.leave();
        this.client.destroy();
        this._connected = false;
        this._voiceUsers.clear();
    }

    // ── Info Getters ─────────────────────────────────────────────────────────

    getUser(): BotUser | null {
        const u = this.client.user;
        if (!u) return null;
        return {
            id: u.id,
            tag: u.tag,
            avatar: u.displayAvatarURL({ dynamic: true }),
        };
    }

    getStatus() {
        return {
            connected: this._connected,
            user: this.getUser(),
            guildId: this._guildId ?? null,
            channelId: this._channelId ?? null,
            streaming: this._streaming,
            voiceUsers: this.getVoiceUsers(),
        };
    }

    getGuilds() {
        return this.client.guilds.cache.map((g) => ({
            id: g.id,
            name: g.name,
            icon: g.iconURL({ dynamic: true, size: 64 }),
        }));
    }

    getChannels(guildId: string) {
        const guild = this.client.guilds.cache.get(guildId);
        if (!guild) throw new Error("Guild not found");
        return guild.channels.cache
            .filter((c) => c.type === "GUILD_VOICE" || c.type === "GUILD_STAGE_VOICE")
            .map((c) => ({
                id: c.id,
                name: c.name,
                type: c.type,
                members: (c as VoiceChannel).members?.size ?? 0,
            }))
            .sort((a, b) => a.name.localeCompare(b.name));
    }

    getVoiceUsers(): VoiceUser[] {
        return Array.from(this._voiceUsers.values());
    }

    /** Debug info about audio streams */
    getAudioStats() {
        const stats: Record<string, any> = {};
        for (const [userId, ua] of this._userAudio) {
            const user = this._voiceUsers.get(userId);
            stats[userId] = {
                displayName: user?.displayName ?? userId,
                streamActive: ua.stream !== null && !ua.stream?.destroyed,
                volume: ua.volume,
                bytesReceived: ua.bytesReceived,
                subscribers: ua.subscribers.size,
            };
        }
        return {
            voiceConnected: !!this._voiceConn,
            encryptionMode: (this._voiceConn as any)?.authentication?.mode ?? null,
            ssrcMapSize: (this._voiceConn as any)?.ssrcMap?.size ?? 0,
            streams: stats,
        };
    }

    // ── Voice (selfbot native – for audio receiving) ───────────────────────

    async join(guildId: string, channelId: string): Promise<void> {
        if (this._channelId) await this.leave();

        const channel = this.client.channels.cache.get(channelId) as
            | VoiceChannel
            | StageChannel
            | undefined;
        if (!channel) throw new Error("Channel not found in cache");

        // Use the selfbot's native voice connection (has audio receiver)
        const conn: VoiceConnection = await (
            this.client.voice as any
        ).joinChannel(channel, {
            selfMute: false,
            selfDeaf: false,
            selfVideo: false,
        });

        this._voiceConn = conn;
        this._guildId = guildId;
        this._channelId = channelId;
        this._audioDebugLogged = false;

        // Handle stage channels
        if (channel.type === "GUILD_STAGE_VOICE") {
            try {
                await this.client.user?.voice?.setSuppressed(false);
            } catch {
                /* may not have perms */
            }
        }

        // Listen for speaking events on this connection
        conn.on("speaking", (user: any, speaking: any) => {
            if (!user) return; // user not in cache
            const vu = this._voiceUsers.get(user.id);
            if (vu) vu.speaking = Boolean(speaking?.bitfield);
            // Auto-create audio stream when user starts speaking
            if (speaking?.bitfield) {
                this._startUserAudio(user.id);
            }
        });

        // Debug: log when audio packets arrive
        conn.receiver?.on("receiverData", (ssrcData: any) => {
            if (!this._audioDebugLogged) {
                console.log(`[audio] First packet received from user ${ssrcData?.userId}`);
                this._audioDebugLogged = true;
            }
        });

        // Force the guild member cache to populate before reading the channel
        try {
            const guild = this.client.guilds.cache.get(guildId);
            if (guild) await guild.members.fetch();
        } catch {
            /* partial fetch is fine */
        }

        this._refreshVoiceUsers();

        // Members cache may still be populating — poll until stable
        let retries = 0;
        let lastSize = this._voiceUsers.size;
        const pollMembers = setInterval(() => {
            this._refreshVoiceUsers();
            retries++;
            const settled = this._voiceUsers.size > 0 && this._voiceUsers.size === lastSize;
            lastSize = this._voiceUsers.size;
            if (settled || retries >= 10) {
                clearInterval(pollMembers);
                if (this._voiceUsers.size > 0) {
                    console.log(`[voice] Found ${this._voiceUsers.size} user(s) after ${retries} poll(s)`);
                    // Auto-start audio for all current members
                    for (const userId of this._voiceUsers.keys()) {
                        this._startUserAudio(userId);
                    }
                }
            }
        }, 500);

        console.log(`Joined voice: guild=${guildId} channel=${channelId}`);
    }

    async leave(): Promise<void> {
        if (this._streaming) await this.stopStream();
        this._stopAllAudio();
        if (this._voiceConn) {
            try {
                this._voiceConn.disconnect();
            } catch {
                /* already disconnected */
            }
            this._voiceConn = undefined;
        }
        this._guildId = undefined;
        this._channelId = undefined;
        this._voiceUsers.clear();
        console.log("Left voice channel");
    }

    // ── Per-User Audio ───────────────────────────────────────────────────────

    private _startUserAudio(userId: string): void {
        // If we already have an active stream, don't recreate
        const existing = this._userAudio.get(userId);
        if (existing?.stream && !existing.stream.destroyed) return;
        if (!this._voiceConn?.receiver) return;

        try {
            // Use end:'silence' — stream auto-closes after 250ms silence.
            // We re-create it on the next 'speaking' event.
            const pcmStream = this._voiceConn.receiver.createStream(userId, {
                mode: "pcm",
                end: "silence",
            });

            // Preserve existing subscribers and volume from previous stream
            const prevUa = this._userAudio.get(userId);
            const ua: UserAudio = {
                stream: pcmStream,
                volume: prevUa?.volume ?? this._voiceUsers.get(userId)?.volume ?? 1.0,
                subscribers: prevUa?.subscribers ?? new Set(),
                bytesReceived: prevUa?.bytesReceived ?? 0,
                lastDataTime: Date.now(),
            };

            pcmStream.on("data", (chunk: Buffer) => {
                ua.bytesReceived += chunk.length;
                ua.lastDataTime = Date.now();
                const scaled = applyVolume(chunk, ua.volume);
                for (const sub of ua.subscribers) {
                    if (!sub.destroyed) sub.write(scaled);
                }
            });

            pcmStream.on("end", () => {
                // Pad with silence to smooth the transition when user stops
                for (const sub of ua.subscribers) {
                    if (!sub.destroyed) {
                        for (let i = 0; i < 5; i++) sub.write(this._silenceBuf);
                    }
                }
                ua.stream = null;
            });

            pcmStream.on("error", (err: Error) => {
                console.error(`[audio] Stream error for ${userId}:`, err.message);
                ua.stream = null;
            });

            this._userAudio.set(userId, ua);
            if (!prevUa) {
                console.log(`[audio] Stream created for user ${userId}`);
            }
        } catch (err: any) {
            console.error(
                `[audio] Failed to create stream for ${userId}:`,
                err.message,
            );
        }
    }

    private _stopUserAudio(userId: string): void {
        const ua = this._userAudio.get(userId);
        if (!ua) return;
        try {
            if (ua.stream && !ua.stream.destroyed) ua.stream.destroy();
        } catch {
            /* ignore */
        }
        for (const sub of ua.subscribers) {
            try {
                sub.end();
            } catch {
                /* ignore */
            }
        }
        ua.subscribers.clear();
        this._userAudio.delete(userId);
    }

    private _stopAllAudio(): void {
        for (const userId of this._userAudio.keys()) {
            this._stopUserAudio(userId);
        }
        this._stopSilencePump();
        this._audioDebugLogged = false;
    }

    /**
     * Silence pump: writes 20ms silence frames to subscribers when no audio
     * data has arrived recently, keeping the WAV stream continuous and
     * preventing crackling/pops from stream gaps.
     */
    private _startSilencePump(): void {
        if (this._silenceTimer) return;
        this._silenceTimer = setInterval(() => {
            const now = Date.now();
            for (const [, ua] of this._userAudio) {
                if (ua.subscribers.size === 0) continue;
                if (now - ua.lastDataTime > 60) {
                    for (const sub of ua.subscribers) {
                        if (!sub.destroyed) sub.write(this._silenceBuf);
                    }
                }
            }
        }, 20);
    }

    private _stopSilencePump(): void {
        if (this._silenceTimer) {
            clearInterval(this._silenceTimer);
            this._silenceTimer = undefined;
        }
    }

    /**
     * Subscribe to a user's audio as a WAV stream.
     * Returns a PassThrough that emits 48kHz stereo 16-bit PCM with WAV header.
     * Data flows whenever the user is speaking; silence gaps are normal.
     */
    subscribeAudio(userId: string): PassThrough | null {
        let ua = this._userAudio.get(userId);
        if (!ua) {
            // Create a placeholder entry — actual stream starts on speaking event
            ua = { stream: null, volume: 1.0, subscribers: new Set(), bytesReceived: 0, lastDataTime: 0 };
            this._userAudio.set(userId, ua);
        }

        const pass = new PassThrough();
        pass.write(wavHeader());
        ua.subscribers.add(pass);
        this._startSilencePump();
        pass.on("close", () => {
            ua!.subscribers.delete(pass);
            // Stop pump if no subscribers anywhere
            let total = 0;
            for (const [, a] of this._userAudio) total += a.subscribers.size;
            if (total === 0) this._stopSilencePump();
        });
        return pass;
    }

    /** Set volume for a specific user (0.0–2.0) */
    setUserVolume(userId: string, volume: number): void {
        const clamped = Math.max(0, Math.min(2, volume));
        const ua = this._userAudio.get(userId);
        if (ua) ua.volume = clamped;
        const vu = this._voiceUsers.get(userId);
        if (vu) vu.volume = clamped;
    }

    /** Get volume for a specific user */
    getUserVolume(userId: string): number {
        return (
            this._userAudio.get(userId)?.volume ??
            this._voiceUsers.get(userId)?.volume ??
            1.0
        );
    }

    // ── Go Live Streaming ────────────────────────────────────────────────────

    /**
     * Start a Go Live stream from any ffmpeg-compatible source URL.
     */
    async startStream(sourceUrl: string, opts: StreamOptions = {}): Promise<void> {
        if (this._streaming) throw new Error("Already streaming");
        if (!this._guildId || !this._channelId)
            throw new Error("Not in a voice channel");

        if (!this._voiceConn) {
            throw new Error("Cannot start stream without a selfbot voice connection. Use tryJoin first.");
        }

        // Mock the Discord-Video-Stream's internal voiceConnection so it can call createStream
        // without destroying our existing discord.js-selfbot-v13 connection!
        const guild = this.client.guilds.cache.get(this._guildId);
        const session_id = guild?.me?.voice.sessionId || (this._voiceConn as any)?.authentication?.sessionId;
        
        if (!session_id) {
            throw new Error("Could not find session ID for voice connection");
        }

        // Inject a fake voice connection into the streamer
        // The streamer class strictly needs primitive properties to call createStream().
        (this.streamer as any)._voiceConnection = {
            guildId: this._guildId,
            channelId: this._channelId,
            session_id: session_id,
            botId: this.client.user!.id,
            type: "guild",
            // StreamConnection attaches itself back here via this.voiceConnection.streamConnection
        };

        // We don't call this._voiceConn.disconnect() anymore! The selfbot stays in the channel
        // and continues receiving audio!

        this._streaming = true;
        this._streamAbort = new AbortController();
        const signal = this._streamAbort.signal;

        const isSrt = sourceUrl.startsWith("srt://");

        try {
            let output: Readable;

            const encoder = Encoders.software({
                x264: { preset: "superfast" },
                x265: { preset: "superfast" },
            });

            const stream = prepareStream(
                sourceUrl,
                {
                    encoder,
                    height: opts.height || 1080,
                    frameRate: opts.fps || 30,
                    bitrateVideo: opts.bitrate || 5000,
                    bitrateVideoMax: opts.bitrateMax || 7500,
                    videoCodec: Utils.normalizeVideoCodec(
                        (opts.codec || "H264") as any,
                    ),
                    noTranscoding: true, // IMPORTANT: Never re-encode the video stream from OBS! This eliminates the transcode delay and 1fps constraint.
                    minimizeLatency: true,
                    // Aggressive tuning for absolute minimal delay and probe
                    customInputOptions: [
                        "-probesize 32",
                        ...isSrt ? [
                            "-latency 1000",
                            "-scan_all_pmts 0",
                            "-fflags nobuffer+flush_packets",
                            "-flags low_delay"
                        ] : [],
                    ],
                    includeAudio: true,
                    bitrateAudio: 192,
                },
                signal,
            );

            stream.command.on("error", (err: Error) => {
                console.error("FFmpeg Go Live error:", err.message);
            });

            output = stream.output;

            playStream(
                output,
                this.streamer,
                { type: "go-live", readrateInitialBurst: 1000 },
                signal,
            )
                .then(() => {
                    console.log("Go Live stream ended naturally");
                    this._streaming = false;
                    this._cleanupIngest();
                    this.streamer.stopStream(); // Instead of rejoinForAudio, just cleanly end streamer's Go Live
                })
                .catch((err) => {
                    if (err.name !== "AbortError") {
                        console.error("Go Live error:", err);
                    }
                    this._streaming = false;
                    this._cleanupIngest();
                    this.streamer.stopStream();
                });

            console.log(`Go Live started: ${sourceUrl}`);
        } catch (err) {
            this._streaming = false;
            this._cleanupIngest();
            this.streamer.stopStream();
            throw err;
        }
    }

    private _cleanupIngest(): void {
        if (this._ingestProcess) {
            try { this._ingestProcess.kill("SIGTERM"); } catch { /* ok */ }
            this._ingestProcess = undefined;
        }
    }

    async stopStream(): Promise<void> {
        this._streamAbort?.abort();
        try { this.streamer.stopStream(); } catch { /* ok */ }
        // Do not call leaveVoice() because we are piggybacking on the selfbot voice connection
        // try { this.streamer.leaveVoice(); } catch { /* ok */ }
        this._streaming = false;
        console.log("Go Live stopped");
        // No need to rejoin, as we never left the selfbot voice connection!
        // await this._rejoinForAudio();
    }

    /** After Go Live ends, re-join via selfbot for audio receiving */
    private async _rejoinForAudio(): Promise<void> {
        if (!this._guildId || !this._channelId) return;
        try {
            const guildId = this._guildId;
            const channelId = this._channelId;
            this._voiceConn = undefined;
            this._guildId = undefined;
            this._channelId = undefined;
            await this.join(guildId, channelId);
            console.log("Rejoined voice for audio receiving");
        } catch (err: any) {
            console.error("Failed to rejoin for audio:", err.message);
        }
    }

    // ── Internal ─────────────────────────────────────────────────────────────

    private _refreshVoiceUsers() {
        if (!this._guildId || !this._channelId) return;

        const guild = this.client.guilds.cache.get(this._guildId);
        if (!guild) return;

        const ch = guild.channels.cache.get(this._channelId);
        if (!ch || !("members" in ch)) return;

        const vc = ch as VoiceChannel | StageChannel;
        // Track which members are currently in the channel
        const currentIds = new Set<string>();
        for (const [id, member] of vc.members) {
            if (id === this.client.user?.id) continue;
            currentIds.add(id);
            if (!this._voiceUsers.has(id)) {
                this._voiceUsers.set(id, {
                    id,
                    username: member.user.username,
                    displayName: member.displayName,
                    avatar: member.user.displayAvatarURL({ dynamic: true, size: 128 }),
                    speaking: false,
                    volume: this._userAudio.get(id)?.volume ?? 1.0,
                });
            }
        }
        // Remove users who left
        for (const id of this._voiceUsers.keys()) {
            if (!currentIds.has(id)) {
                this._stopUserAudio(id);
                this._voiceUsers.delete(id);
            }
        }
    }

    private _setupListeners() {
        this.client.on("voiceStateUpdate", (oldState, newState) => {
            const selfId = this.client.user?.id;

            // Someone joined our channel
            if (
                newState.channelId === this._channelId &&
                newState.id !== selfId
            ) {
                const member = newState.member;
                if (member) {
                    this._voiceUsers.set(member.id, {
                        id: member.id,
                        username: member.user.username,
                        displayName: member.displayName,
                        avatar: member.user.displayAvatarURL({
                            dynamic: true,
                            size: 128,
                        }),
                        speaking: false,
                        volume: this._userAudio.get(member.id)?.volume ?? 1.0,
                    });
                    this._startUserAudio(member.id);
                }
            }

            // Someone left our channel
            if (
                oldState.channelId === this._channelId &&
                newState.channelId !== this._channelId
            ) {
                this._stopUserAudio(oldState.id);
                this._voiceUsers.delete(oldState.id);
            }
        });
    }
}
