import { io } from 'socket.io-client'
import { execa } from 'execa'
import { BaseCommand, args } from '@adonisjs/core/ace'
import { DateTime } from 'luxon'
import { getRtspStreamCharacteristics } from '#utilities/rtsp'
import { getHostnameFromRtspUrl } from '#utilities/url'
import { pickPort } from '#utilities/ports'
import { RTCPeerConnection, RTCRtpCodecParameters } from 'werift'
import { IceCandidateError } from '#services/ice'
import { createSocket } from 'node:dgram'
import { EventEmitter } from 'node:events'
import env from '#start/env'
import Camera from '#models/camera'
import { createServer } from 'node:net'
import { writeFile, unlink } from 'node:fs/promises'
import {
  getHardwareAcceleratedDecodingArgumentsFor,
  getHardwareAcceleratedEncodingArgumentsFor,
} from '#utilities/ffmpeg'
import { buildOutputStreamerArgs, buildCameraFfmpegArgs, buildRtspCameraFfmpegArgs, getRtspCharacteristicsRetryDelayMs } from '#utilities/streamer_args'
import { subProcessLogger as logger } from '#services/logger'

import type { CommandOptions } from '@adonisjs/core/types/ace'
import type { ExecaChildProcess } from 'execa'
import type { smartdevicemanagement_v1 } from 'googleapis'
import type { Socket as StreamPrivateApiClient } from 'socket.io-client'
import type { RTCIceServer, RTCTrackEvent, RTCRtpReceiver } from 'werift'
import type { PickPortOptions } from '#utilities/ports'
import type { Socket as DGramSocket } from 'node:dgram'
import type { Server as UnixSocketServer, Socket as UnixSocket } from 'node:net'
import type winston from 'winston'

export default class NestmtxStream extends BaseCommand {
  static commandName = 'nestmtx:stream'
  static description = 'Start a stream to the MediaMTX server'

  static options: CommandOptions = {
    startApp: true,
  }

  // ─── Timeouts ────────────────────────────────────────────────────────────────
  static readonly #STATIC_STREAMER_KILL_MS = 2000
  static readonly #OUTPUT_STREAMER_SIGKILL_MS = 1500
  static readonly #OUTPUT_STREAMER_HARD_KILL_MS = 3000
  static readonly #PLACEHOLDER_EXIT_WAIT_MS = 1500
  static readonly #VBV_UNDERFLOW_CLEAR_MS = 10000
  static readonly #VBV_UNDERFLOW_TRIGGER_S = 10
  static readonly #RATE_LIMIT_RETRY_MS = 30000
  static readonly #RTSP_CHARACTERISTICS_TIMEOUT_MS = 30000
  static readonly #DIAGNOSTIC_DELAY_MS = 2000
  static readonly #WEBRTC_CONNECTION_TIMEOUT_MS = 30000
  static readonly #SSRC_SAMPLE_PACKETS = 10
  // ─────────────────────────────────────────────────────────────────────────────

  @args.string({ description: 'The path to start the stream for' })
  declare path: string

  @args.string({
    description:
      'The port on which the nestmtx streamer private API is awaiting for connections on',
  })
  declare port: string

  #bus: EventEmitter = new EventEmitter({
    captureRejections: true,
  })

  #api?: StreamPrivateApiClient
  #streamerSocket?: UnixSocketServer
  #cameraSocket?: UnixSocketServer
  #udpSocket?: DGramSocket
  #streamer?: ExecaChildProcess
  #staticStreamer?: ExecaChildProcess
  #cameraStreamer?: ExecaChildProcess
  #abortController: AbortController = new AbortController()
  #connectingStreamAbortController: AbortController = new AbortController()

  #iceServers: RTCIceServer[] = []
  #additionalHostAddresses: string[] = []
  #rtspCameraStreamUrl?: string

  #packetsToOutputCount: number = 0
  #packetsToOutputInterval?: NodeJS.Timeout
  #lastThirtyPacketCounts: number[] = []
  #stalled: boolean = false
  #outputStreamerIsRestarting: boolean = false
  #firstUnderflowWarningAt?: DateTime
  #lastUnderflowWarningAt?: DateTime
  #clearUnderflowWarningInterval?: NodeJS.Timeout

  // Local SRT relay port: camera ffmpeg publishes MPEG-TS here as an SRT listener;
  // the VAAPI output streamer reads from it as a caller. Set in #webrtcStart (restart-
  // based mode only) before camera ffmpeg is spawned. Undefined in single-process mode
  // and for the RTSP path, which both continue to use the Unix socket → pipe:3 path.
  #cameraRelayPort?: number

  // Single-process mode state: track reinit failures so we can recover in-place
  // rather than crashing the whole process, and disable the mode after repeated failures.
  #singleProcessReinitCount: number = 0
  #singleProcessFailed: boolean = false
  #singleProcessReinitActive: boolean = false

  // Stored across #webrtcStart calls so the output-streamer exit handler can send
  // a PLI after an in-place restart (videoReceiver is a local in #webrtcStart and
  // therefore out of scope from the exit handler closure).
  #videoReceiver?: RTCRtpReceiver
  #videoSsrc?: number

  // Camera stream format detected from output streamer stderr during the live camera
  // phase. Populated after the first in-place restart (when the new output streamer
  // logs the input stream info at info level). Used in subsequent cycles to pre-encode
  // the placeholder at the camera's exact resolution and H264 profile so the VAAPI
  // hardware decoder's filter chain never sees an SPS change.
  #detectedCameraSize?: string        // e.g. "1152x864"
  #detectedCameraProfile?: string     // e.g. "High", "Main"
  #detectedCameraColorspace?: string  // e.g. "bt709" or "bt470bg/unknown/unknown"
  #detectedCameraFrameRate?: number   // e.g. 30
  #detectedCameraRefs?: number        // num_ref_frames, e.g. 3 — passed to libx264 as -refs N
  #detectedCameraLevel?: string       // H264 level string, e.g. "3.1" — passed to libx264 as -level X.Y
  #singleProcessCameraActive: boolean = false
  // Timestamp (ms) of the first format detection in the current camera phase.
  // 0 = not yet detected. Guards against the encoder's own output-side stream-info
  // line (~30ms after the correct input-side line) overwriting the detection: once
  // set, re-detection is blocked for 5 seconds. After that stabilization window,
  // re-detection is allowed so mid-session format changes (e.g., day→night IR mode)
  // can update the stored values. Cleared to 0 when a new camera phase starts.
  #detectedCameraForCurrentCycleAt: number = 0

  get #outputStreamLogger() {
    return logger.child({ stream: 'output' })
  }

  get #cameraStreamLogger() {
    return logger.child({ stream: 'camera' })
  }

  get #staticStreamLogger() {
    return logger.child({ stream: 'static' })
  }

  get #streamerPassthroughSock() {
    return this.app.makePath('resources', `streamer.${process.pid}.sock`)
  }

  get #cameraPassthroughSock() {
    return this.app.makePath('resources', `camera.${process.pid}.sock`)
  }

  get #streamerFFMpegInputSdp() {
    return this.app.makePath('resources', `streamer.${process.pid}.sdp`)
  }

  get #noSuchCameraFilePath() {
    return this.app.makePath('resources/mediamtx/no-such-camera.jpg')
  }

  get #connectingFilePath() {
    return this.app.makePath('resources/mediamtx/connecting.jpg')
  }

  get #cameraDisabledFilePath() {
    return this.app.makePath('resources/mediamtx/camera-disabled.jpg')
  }

  get #destination() {
    return `srt://127.0.0.1:${env.get('MEDIA_MTX_SRT_PORT', 8890)}/?streamid=publish:${this.path}&pkt_size=1316`
  }

  get #hardwareAcceleratedDecodingArguments() {
    return getHardwareAcceleratedDecodingArgumentsFor(
      env.get('FFMPEG_HW_ACCELERATOR', ''),
      env.get('FFMPEG_HW_ACCELERATOR_DEVICE', '')
    )
  }

  get #hardwareAcceleratedEncodingArguments() {
    return getHardwareAcceleratedEncodingArgumentsFor(
      env.get('FFMPEG_HW_ACCELERATOR', ''),
      env.get('FFMPEG_HW_ACCELERATOR_DEVICE', '')
    )
  }

  get #isVaapiEnabled() {
    return env.get('FFMPEG_HW_ACCELERATOR', '').toLowerCase() === 'vaapi'
  }

  get #isHwAccelEnabled() {
    return env.get('FFMPEG_HW_ACCELERATOR', '') !== ''
  }

  // Experimental single-process investigation mode. When VAAPI_SINGLE_PROCESS=true
  // (and FFMPEG_HW_ACCELERATOR=vaapi is also set), the output streamer starts in
  // VAAPI mode from the very beginning of a camera session and is never restarted
  // on placeholder↔live source switches. This tests whether the VAAPI hardware
  // decoder can handle the SPS change that occurs when the static placeholder stream
  // is replaced by the live camera stream, without a filter chain reinitialization.
  //
  // If the decoder survives the transition, the entire restart-based architecture
  // (Rounds 2–14) becomes unnecessary: a single continuous output process and SRT
  // connection can serve both the placeholder and live phases, eliminating the
  // viewer session drop that has been accepted as architectural since Round 14.
  //
  // If "Error reinitializing filters, Function not implemented" appears in the
  // output streamer log after the source switches, then the SPS parameters differ
  // and placeholder pre-conversion (matching the camera's exact profile/level) is
  // the next step. Set FFMPEG_DEBUG_LEVEL=info in the same test run to see the
  // actual stream parameters the output streamer detects for each source.
  get #singleProcessMode() {
    return (
      !this.#singleProcessFailed &&
      String(env.get('VAAPI_SINGLE_PROCESS', 'false')) === 'true' &&
      this.#isVaapiEnabled
    )
  }

  // When enabled, the output streamer starts once and never restarts.
  // It reads from pipe:3 in -c:v copy mode — no decode/encode, no filter chain.
  // The placeholder and camera ffmpeg write to the same Unix socket relay
  // sequentially; NestMTX holds the write end open so the output streamer
  // never sees EOF during the switch. MediaMTX always has a publisher, so
  // VLC (or any RTSP client) never gets its session dropped.
  get #persistentMode() {
    return String(env.get('NESTMTX_PERSISTENT_STREAMER', 'false')) === 'true'
  }

  async run() {
    process.once('SIGINT', this.#gracefulExit.bind(this))
    logger.info(`NestMTX Streamer for "${this.path}". PID: ${process.pid}`)
    logger.info(`Cleaning up old files`)

    const filesToCleanup = [
      this.#streamerPassthroughSock,
      this.#cameraPassthroughSock,
      this.#streamerFFMpegInputSdp,
    ]
    await Promise.all(
      filesToCleanup.map(async (f) => {
        try {
          await unlink(f)
        } catch (e) {
          logger.error(`Failed to delete ${f}: ${e.message}`)
        }
      })
    )
    this.#abortController.signal.addEventListener('abort', () => {
      if (this.#packetsToOutputInterval) {
        clearInterval(this.#packetsToOutputInterval)
      }
    })
    logger.info(`Starting Unix Sockets`)
    this.#streamerSocket = createServer(this.#onStreamerUnixSocketConnection.bind(this))
    this.#streamerSocket.listen(this.#streamerPassthroughSock)
    this.#cameraSocket = createServer(this.#onCameraUnixSocketConnection.bind(this))
    this.#cameraSocket.listen(this.#cameraPassthroughSock)
    this.#streamerSocket.on('error', (error) => {
      logger.error(`Error from Streamer Unix Socket: ${error.message}`)
    })
    this.#cameraSocket.on('error', (error) => {
      logger.error(`Error from Camera Unix Socket: ${error.message}`)
    })
    logger.info(`Starting output streamer`)
    if (this.#persistentMode) {
      logger.info(`NESTMTX_PERSISTENT_STREAMER: enabled — output streamer will run in copy mode and never restart`)
    } else if (this.#singleProcessMode) {
      logger.info(`VAAPI_SINGLE_PROCESS mode: enabled — starting output streamer in VAAPI mode from the beginning`)
    } else {
      logger.info(`VAAPI_SINGLE_PROCESS mode: disabled — using restart-based VAAPI switch`)
    }
    this.#startOutputStreamer(this.#singleProcessMode)
    const privateApiServerUrl = `http://127.0.0.1:${this.port}`
    logger.info(`Searching for Private API Server`)
    await new Promise<void>((resolve) => {
      this.#api = io(privateApiServerUrl, {
        autoConnect: false,
        reconnection: false,
        timeout: 1000,
      })
      this.#api.once('error', () => {
        logger.error(`Private API Server not found`)
        process.exit(1)
      })
      this.#api.once('connect', () => {
        logger.info(`Private API Server connected`)
      })
      this.#api.once('disconnect', () => {
        logger.error(`Private API Server disconnected`)
        process.exit(1)
      })
      this.#api.on('test:stall', () => {
        this.#bus.emit('stall')
      })
      this.#api.on(`${this.path}:stall`, () => {
        this.#bus.emit('stall')
      })
      Promise.all([
        new Promise<void>((r) => {
          this.#api!.once('ice', (iceServers: RTCIceServer[]) => {
            this.#iceServers = iceServers
            logger.info(`ICE Servers configured`)
            r(void 0)
          })
        }),
        new Promise<void>((r) => {
          this.#api!.once('hosts', (additionalHostAddresses: string[]) => {
            this.#additionalHostAddresses = additionalHostAddresses
            logger.info(`Hosts configured`)
            r(void 0)
          })
        }),
      ]).then(() => {
        resolve()
      })
      this.#api.connect()
    })
    logger.info(`Searching for Camera`)
    const camera = await Camera.findBy({ mtx_path: this.path })
    this.#packetsToOutputInterval = setInterval(() => {
      const packets = this.#packetsToOutputCount
      this.#api!.emit('packetRate', packets)
      this.#packetsToOutputCount = 0
      this.#lastThirtyPacketCounts.push(packets)
      if (this.#lastThirtyPacketCounts.length > 30) {
        this.#lastThirtyPacketCounts.shift()
      }
      // In SRT relay mode (camera phase) data flows camera ffmpeg → SRT → output
      // streamer without touching Node, so #packetsToOutputCount stays at zero even
      // when the stream is healthy. Skip the packet-count stall check for this case;
      // camera crashes are already detected by the camera ffmpeg exit handler.
      // Use process.kill(pid, 0) to verify the process is still alive: exitCode can
      // lag behind actual process death in rare race conditions, and a dead-but-unkilled
      // zombie would cause us to skip stall detection indefinitely.
      let cameraIsActiveInSrtMode = false
      if (this.#cameraRelayPort !== undefined && this.#cameraStreamer?.exitCode === null) {
        const pid = this.#cameraStreamer?.pid
        if (pid) {
          try { process.kill(pid, 0); cameraIsActiveInSrtMode = true } catch { /* dead */ }
        }
      }
      if (
        !this.#stalled &&
        !cameraIsActiveInSrtMode &&
        this.#lastThirtyPacketCounts.length === 30 &&
        this.#lastThirtyPacketCounts.every((count) => count === 0) &&
        ((this.#staticStreamer && this.#staticStreamer.pid) ||
          (this.#cameraStreamer && this.#cameraStreamer.pid))
      ) {
        logger.warning(`No packets received in the last 30 seconds. Stall detected.`)
        this.#stalled = true
        this.#bus.emit('stall')
      }
    }, 1000)
    if (!camera) {
      logger.info(`Camera not found`)
      this.#connectingStreamAbortController.abort()
      this.#streamJpegToOutputStream(this.#noSuchCameraFilePath)
    } else if (
      !camera.isEnabled ||
      !camera.protocols ||
      (!camera.protocols.includes('WEB_RTC') && !camera.protocols.includes('RTSP'))
    ) {
      logger.info(`Camera disabled`)
      this.#connectingStreamAbortController.abort()
      this.#streamJpegToOutputStream(this.#cameraDisabledFilePath)
    } else {
      await camera.load('credential')
      const service: smartdevicemanagement_v1.Smartdevicemanagement =
        await camera.credential.getSDMClient()
      try {
        if (camera.protocols.includes('WEB_RTC')) {
          await this.#webrtcStart(service, camera)
        } else if (camera.protocols.includes('RTSP')) {
          await this.#rtspStart(service, camera)
        }
      } catch (err) {
        logger.error(err.message)
        process.exit(1)
      }
    }
    this.#bus.on('stall', () => {
      if (this.#staticStreamer) {
        this.#staticStreamer.kill('SIGABRT')
      }
      if (this.#cameraStreamer) {
        this.#cameraStreamer.kill('SIGABRT')
      }
    })
  }

  #validateRtpPacket(packet: Buffer) {
    if (packet.length < 12) {
      return false
    }
    return true
  }

  #onStreamerUnixSocketConnection(socket: UnixSocket) {
    logger.info(`streamer.sock: client connected`)
    let firstWrite = true
    socket.on('data', (raw) => {
      const valid = this.#validateRtpPacket(raw)
      if (!valid) {
        return
      }
      if (this.#streamer && !this.#outputStreamerIsRestarting) {
        // @ts-expect-error - stdio[3] is a net.Socket in pipe mode; absent in SRT relay mode
        const fd3 = this.#streamer.stdio[3]
        if (!fd3) return
        if (firstWrite) {
          firstWrite = false
          this.#outputStreamLogger.info(`First data from static input reaching pipe:3`)
        }
        this.#packetsToOutputCount += 1
        this.#stalled = false
        fd3.write(raw)
      }
    })
    socket.on('end', () => logger.info(`streamer.sock: client disconnected (end)`))
    socket.on('close', () => logger.info(`streamer.sock: client disconnected (close)`))
    socket.on('error', (error) => {
      logger.error(`streamer.sock client error: ${error.message}`)
    })
  }

  #onCameraUnixSocketConnection(socket: UnixSocket) {
    logger.info(`camera.sock: client connected`)
    let firstWrite = true
    socket.on('data', (raw) => {
      const valid = this.#validateRtpPacket(raw)
      if (!valid) {
        return
      }
      if (this.#streamer && !this.#outputStreamerIsRestarting) {
        // @ts-expect-error - stdio[3] is a net.Socket in pipe mode; absent in SRT relay mode
        const fd3 = this.#streamer.stdio[3]
        if (!fd3) return
        if (firstWrite) {
          firstWrite = false
          this.#outputStreamLogger.info(
            `First data from camera input reaching pipe:3 of output streamer pid=${this.#streamer.pid}`
          )
        }
        this.#packetsToOutputCount += 1
        this.#stalled = false
        fd3.write(raw)
      }
    })
    socket.on('end', () => logger.info(`camera.sock: client disconnected (end)`))
    socket.on('close', () => logger.info(`camera.sock: client disconnected (close)`))
    socket.on('error', (error) => {
      logger.error(`camera.sock client error: ${error.message}`)
    })
  }

  #startOutputStreamer(useHwAccel: boolean = false) {
    const ffmpegBinary = env.get('FFMPEG_BIN', 'ffmpeg')
    const hwAccel = useHwAccel ? env.get('FFMPEG_HW_ACCELERATOR', '').toLowerCase() : ''
    const accelDevice = env.get('FFMPEG_HW_ACCELERATOR_DEVICE', '')
    const isVaapi = hwAccel === 'vaapi'
    const isCuda = hwAccel === 'nvenc' || hwAccel === 'nvdec' || hwAccel === 'cuvid'
    const isVideoToolbox = hwAccel === 'videotoolbox'
    const isQsv = hwAccel === 'qsv'

    // Log level selection:
    //  - Single-process VAAPI: 'info' — needed for stream-info lines that drive
    //    format detection (camera resolution/profile for placeholder matching).
    //  - All other modes: env default ('warning', or higher if FFMPEG_DEBUG_LEVEL set).
    //    'verbose' was tried in Round 37 but the expected "Reinit context" hwaccel
    //    signal did not appear even at that level — not worth the log volume.
    const logLevel = (isVaapi && this.#singleProcessMode)
      ? 'info'
      : env.get('FFMPEG_DEBUG_LEVEL', 'warning')

    const useSrtRelay = isVaapi && this.#cameraRelayPort !== undefined

    const ffmpegArgs = buildOutputStreamerArgs({
      logLevel,
      hwAccel,
      accelDevice,
      persistentMode: this.#persistentMode,
      cameraRelayPort: this.#cameraRelayPort,
      destination: this.#destination,
    })

    const useCopyMode = this.#persistentMode

    this.#outputStreamLogger.info(
      `Spawning output streamer: ${ffmpegBinary} ${ffmpegArgs.join(' ')}`
    )
    this.#streamer = execa(ffmpegBinary, ffmpegArgs, {
      // SRT relay mode: output streamer reads from the network, no fd3 needed.
      // Pipe mode: fd3 carries the MPEG-TS stream from the static/camera Unix socket relay.
      stdio: useSrtRelay ? 'pipe' : ['pipe', 'pipe', 'pipe', 'pipe'],
      reject: false,
      shell: false,
      signal: this.#abortController.signal,
    })
    this.#streamer.stdout!.on('data', (data) => {
      data
        .toString()
        .split('\n')
        .map((line: string) => line.trim())
        .filter((line: string) => line.length > 0)
        .forEach((line: string) => {
          this.#outputStreamLogger.info(line)
        })
    })
    this.#streamer.stderr!.on('data', (data) => {
      data
        .toString()
        .split('\n')
        .map((line: string) => line.trim())
        .filter((line: string) => line.length > 0)
        .forEach((line: string) => {
          // Log all output streamer stderr regardless of level — needed to capture
          // VAAPI init failures which may appear as non-ERROR prefixed lines.
          if (line.toLowerCase().includes('error')) {
            this.#outputStreamLogger.error(line)
          } else {
            this.#outputStreamLogger.info(line)
          }
          // Detect filter-chain reinitialization failure in single-process mode.
          // This is the "Error reinitializing filters, Function not implemented" error
          // that occurs when the VAAPI hardware decoder sees a new SPS with different
          // parameters. Set the flag so the exit handler can recover in-place.
          if (this.#singleProcessMode && line.includes('Error reinitializing filters')) {
            this.#singleProcessReinitActive = true
          }
          // During the live camera phase, parse the stream info line that ffmpeg emits
          // when it first processes the input (requires loglevel=info, set above for
          // single-process mode). Format: "Stream #0:0: Video: h264 (High), yuv420p..., 1152x864"
          // Stored values are applied to the next placeholder cycle so the VAAPI decoder
          // never sees an SPS resolution or profile change.
          //
          // Regex uses .*? (lazy) after the profile so it tolerates optional codec-tag
          // fields like " (avc1 / 0x31637661)" that appear between the profile and the
          // pixel format in some container/encoder combinations. \d{3,4}x\d{3,4} matches
          // standard video resolutions (640×480 through 1920×1080) while avoiding false
          // matches on hex codec tags (single leading digit before 'x') or SAR/DAR ratios
          // (which use ':' not 'x').
          if (this.#singleProcessMode && this.#singleProcessCameraActive) {
            const now = Date.now()
            const isFirstDetection = this.#detectedCameraForCurrentCycleAt === 0
            // After the first detection, block re-detection for 5 seconds.
            // The encoder's own output-side stream-info line appears ~30ms after the
            // correct input-side line — both match the same regex, and the encoder's
            // always reports 'High' profile regardless of the camera's actual profile.
            // The 5s window is large enough to exclude the encoder's line (30ms) while
            // still catching genuine mid-session format changes (day/night IR switch)
            // which happen minutes or hours later.
            const isRedetectionWindow = !isFirstDetection &&
              (now - this.#detectedCameraForCurrentCycleAt) > 5000

            if (isFirstDetection || isRedetectionWindow) {
              const m = line.match(/Video: h264 \(([^)]+)\).*?(\d{3,4}x\d{3,4})/)
              if (m) {
                // Profile string may include an @L suffix on some builds (e.g. "Main@L3.1").
                // Strip it so -profile:v receives a clean value. Level and refs are now
                // detected via ffprobe (before camera ffmpeg spawns) — not from this line.
                const rawProfile = m[1]
                const atLIdx = rawProfile.indexOf('@L')
                const newProfile = atLIdx !== -1 ? rawProfile.slice(0, atLIdx).trim() : rawProfile
                const newSize = m[2]

                // Extract colorspace from the pixel format section, e.g.:
                //   "yuv420p(bt709)"                  → "bt709"
                //   "yuv420p(bt470bg/unknown/unknown)" → "bt470bg/unknown/unknown"
                //   "yuv420p"                          → undefined (no parens present)
                const csm = line.match(/yuv\w+\(([^)]+)\)/)
                const newColorspace = csm ? csm[1] : undefined

                // Extract frame rate, e.g. "30 fps" → 30, "29.97 fps" → 29.97
                const frm = line.match(/(\d+(?:\.\d+)?) fps/)
                const newFrameRate = frm ? parseFloat(frm[1]) : undefined

                const prevProfile = this.#detectedCameraProfile
                const prevSize = this.#detectedCameraSize
                const prevColorspace = this.#detectedCameraColorspace

                this.#detectedCameraProfile = newProfile
                this.#detectedCameraSize = newSize
                this.#detectedCameraColorspace = newColorspace
                this.#detectedCameraFrameRate = newFrameRate
                this.#detectedCameraForCurrentCycleAt = now

                if (isFirstDetection) {
                  this.#outputStreamLogger.info(
                    `single-process mode: stored camera format — size=${newSize} profile=${newProfile} colorspace=${newColorspace ?? 'undetected'} fps=${newFrameRate ?? 'undetected'} refs=${this.#detectedCameraRefs ?? 'undetected'} level=${this.#detectedCameraLevel ?? 'undetected'} (from line: ${line.substring(0, 120)})`
                  )
                } else {
                  this.#outputStreamLogger.info(
                    `single-process mode: camera format changed mid-session — size=${newSize} (was ${prevSize ?? 'none'}) profile=${newProfile} (was ${prevProfile ?? 'none'}) colorspace=${newColorspace ?? 'undetected'} (was ${prevColorspace ?? 'undetected'}) fps=${newFrameRate ?? 'undetected'} refs=${this.#detectedCameraRefs ?? 'undetected'} level=${this.#detectedCameraLevel ?? 'undetected'} (from line: ${line.substring(0, 120)})`
                  )
                }
              }
            }
          }
        })
    })
    // Node.js backs stdio[3] with a net.Socket internally. When the process is
    // killed during a restart the socket emits 'error' (ECONNRESET / EPIPE).
    // Without a listener that becomes an uncaught exception and crashes the process.
    // In SRT relay mode fd3 doesn't exist, so skip this handler.
    if (!useSrtRelay) {
      // @ts-expect-error - stdio[3] is a net.Socket at runtime
      this.#streamer.stdio[3].on('error', (err: Error) => {
        if (!this.#outputStreamerIsRestarting) {
          this.#outputStreamLogger.error(`Output pipe fd3 error: ${err.message}`)
        }
      })
    }
    this.#streamer.on('exit', async (code) => {
      logger.info(`Streamer exited with code ${code}`)
      if (this.#outputStreamerIsRestarting) {
        return
      }
      // Single-process mode: a filter-chain reinit failure causes the output streamer
      // to exit. Restart it in-place rather than tearing down the whole process so
      // the camera ffmpeg (still running) can resume feeding data after a PLI.
      // Without this, pm3 would respawn the entire per-camera process every 15-20s
      // indefinitely, since the reinit failure is deterministic.
      if (this.#singleProcessReinitActive) {
        this.#singleProcessReinitActive = false
        this.#singleProcessReinitCount++
        if (this.#singleProcessReinitCount >= 3) {
          this.#singleProcessFailed = true
          logger.warning(
            `single-process mode: ${this.#singleProcessReinitCount} consecutive reinit failures — disabling for this session, subsequent cycles will use restart-based mode (detectedSize=${this.#detectedCameraSize ?? 'none'} detectedProfile=${this.#detectedCameraProfile ?? 'none'} detectedColorspace=${this.#detectedCameraColorspace ?? 'none'} detectedRefs=${this.#detectedCameraRefs ?? 'none'} detectedLevel=${this.#detectedCameraLevel ?? 'none'})`
          )
        } else {
          logger.warning(
            `single-process mode: reinit failure #${this.#singleProcessReinitCount} — restarting output streamer in-place (detectedSize=${this.#detectedCameraSize ?? 'none'} detectedProfile=${this.#detectedCameraProfile ?? 'none'} detectedColorspace=${this.#detectedCameraColorspace ?? 'none'} detectedRefs=${this.#detectedCameraRefs ?? 'none'} detectedLevel=${this.#detectedCameraLevel ?? 'none'})`
          )
        }
        // Restart with write-gating so camera data doesn't stream into a half-dead pipe.
        // Gate is released on first stderr from the new process (see #restartOutputStreamer
        // for the same pattern); timeout fallback in case ffmpeg is silent at startup.
        this.#outputStreamerIsRestarting = true
        this.#startOutputStreamer(true)
        logger.info(
          `single-process mode: output streamer restarted (pid=${this.#streamer?.pid}) after reinit failure`
        )
        const spGateTimeout = setTimeout(
          () => { this.#outputStreamerIsRestarting = false },
          NestmtxStream.#PLACEHOLDER_EXIT_WAIT_MS
        )
        this.#streamer!.stderr!.once('data', () => {
          clearTimeout(spGateTimeout)
          this.#outputStreamerIsRestarting = false
        })
        // Request a fresh IDR + SPS/PPS so the new output streamer can decode cleanly.
        if (this.#videoReceiver !== undefined && this.#videoSsrc !== undefined) {
          this.#videoReceiver.sendRtcpPLI(this.#videoSsrc).catch(() => {})
        }
        return
      }
      if (code !== 0 && code !== 8) {
        const res = await this.#streamer
        if (res) {
          logger.info(res.escapedCommand)
        }
      }
      this.#gracefulExit(code || 0)
    })
  }

  async #restartOutputStreamer(useHwAccel: boolean) {
    // Kill the static placeholder explicitly before touching the output streamer.
    // With shell:false the execa kill now hits ffmpeg directly (no shell orphan),
    // but we still need to wait for the process to actually exit so no lingering
    // data reaches pipe:3 of the new output streamer.
    // Only done for the HW-accel transition (useHwAccel=true); when switching back to
    // software, #webrtcStart creates a new #staticStreamer concurrently and we
    // must not kill it.
    if (useHwAccel && this.#staticStreamer) {
      const ss = this.#staticStreamer
      if (ss.exitCode === null) {
        logger.info(`restartOutputStreamer: killing static streamer (pid ${ss.pid})`)
        await new Promise<void>((resolve) => {
          const timeout = setTimeout(() => {
            logger.info(`restartOutputStreamer: static streamer kill timed out`)
            resolve()
          }, NestmtxStream.#STATIC_STREAMER_KILL_MS)
          ss.once('exit', (code) => {
            clearTimeout(timeout)
            logger.info(`restartOutputStreamer: static streamer exited with code ${code}`)
            resolve()
          })
          try {
            ss.kill('SIGTERM')
          } catch {
            // ESRCH: process already dead
            clearTimeout(timeout)
            logger.info(`restartOutputStreamer: static streamer already dead`)
            resolve()
          }
        })
      } else {
        logger.info(`restartOutputStreamer: static streamer already exited (code ${ss.exitCode})`)
      }
      this.#staticStreamer = undefined
    }

    if (this.#streamer) {
      logger.info(`restartOutputStreamer: killing output streamer, useHwAccel=${useHwAccel}`)
      this.#outputStreamerIsRestarting = true
      await new Promise<void>((resolve) => {
        const streamer = this.#streamer!
        let sigkillSent = false

        // After 1.5s without exit, escalate to SIGKILL. The VAAPI hardware decoder
        // can get wedged on corrupted input and stop responding to SIGTERM; SIGKILL
        // is unconditional and ensures the old process is fully dead before the
        // replacement starts, preventing any /dev/dri/renderD128 contention window.
        const sigkillTimeout = setTimeout(() => {
          if (streamer.exitCode !== null) return
          sigkillSent = true
          logger.info(
            `restartOutputStreamer: output streamer SIGTERM unresponsive after ${NestmtxStream.#OUTPUT_STREAMER_SIGKILL_MS}ms, escalating to SIGKILL (pid ${streamer.pid})`
          )
          try {
            streamer.kill('SIGKILL')
          } catch {}
        }, NestmtxStream.#OUTPUT_STREAMER_SIGKILL_MS)

        // Hard safety valve in case SIGKILL doesn't produce an exit event (zombie).
        const hardTimeout = setTimeout(() => {
          clearTimeout(sigkillTimeout)
          logger.info(
            `restartOutputStreamer: output streamer hard kill timeout (pid ${streamer.pid})`
          )
          resolve()
        }, NestmtxStream.#OUTPUT_STREAMER_HARD_KILL_MS)

        streamer.once('exit', (code) => {
          clearTimeout(sigkillTimeout)
          clearTimeout(hardTimeout)
          logger.info(
            `restartOutputStreamer: output streamer exited with code ${code}${sigkillSent ? ' (after SIGKILL escalation)' : ''}`
          )
          resolve()
        })

        try {
          streamer.kill('SIGTERM')
        } catch {
          // ESRCH: process already dead
          clearTimeout(sigkillTimeout)
          clearTimeout(hardTimeout)
          logger.info(`restartOutputStreamer: output streamer already dead`)
          resolve()
        }
      })
    }
    logger.info(`restartOutputStreamer: starting new output streamer (useHwAccel=${useHwAccel})`)
    this.#startOutputStreamer(useHwAccel)
    logger.info(`restartOutputStreamer: new output streamer pid=${this.#streamer?.pid}`)
    // Release the write gate once the new output streamer is actually initialised.
    // In pipe:3 mode we wait for its first stderr output (ffmpeg banner or first warning)
    // rather than clearing immediately after spawn — this prevents the Unix socket relay
    // from writing partial MPEG-TS into pipe:3 before ffmpeg has opened its input buffers.
    // A timeout fallback ensures the gate is never held forever if ffmpeg is silent at startup.
    // In SRT relay mode fd3 is absent and writes bypass Node entirely, so the gate is moot.
    if (this.#streamer?.stdio[3]) {
      const gateTimeout = setTimeout(
        () => { this.#outputStreamerIsRestarting = false },
        NestmtxStream.#PLACEHOLDER_EXIT_WAIT_MS
      )
      this.#streamer!.stderr!.once('data', () => {
        clearTimeout(gateTimeout)
        this.#outputStreamerIsRestarting = false
      })
    } else {
      this.#outputStreamerIsRestarting = false
    }
  }

  #onFFMpegCameraStreamOutput(data: Buffer, log: winston.Logger) {
    data
      .toString()
      .split('\n')
      .map((line: string) => line.trim())
      .filter((line: string) => line.length > 0)
      .forEach((line: string) => {
        if (line.match(/^(.*)\s+VBV\s+underflow\s+\(frame\s+\d+,\s+\-?\d+\s+bits\)$/gm)) {
          if (this.#clearUnderflowWarningInterval) {
            clearTimeout(this.#clearUnderflowWarningInterval)
          }
          if (!this.#firstUnderflowWarningAt) {
            this.#firstUnderflowWarningAt = DateTime.utc()
          }
          this.#lastUnderflowWarningAt = DateTime.utc()
          this.#clearUnderflowWarningInterval = setTimeout(() => {
            this.#firstUnderflowWarningAt = undefined
            this.#lastUnderflowWarningAt = undefined
          }, NestmtxStream.#VBV_UNDERFLOW_CLEAR_MS)
          const duration = this.#lastUnderflowWarningAt.diff(this.#firstUnderflowWarningAt)
          if (duration.as('seconds') > NestmtxStream.#VBV_UNDERFLOW_TRIGGER_S) {
            log.warning('VBV underflow detected for more than 10 seconds. Sending "stall" signal')
            this.#bus.emit('stall')
          }
        }
        if (line.includes('Last message repeated 3')) {
          return
        }
        // Distinguish bitstream corruption from generic camera ffmpeg noise so
        // future test logs immediately identify this specific failure mode.
        if (
          line.includes('h264 bitstream malformed') ||
          line.includes('no startcode found') ||
          line.includes('Error submitting a packet to the muxer')
        ) {
          log.warning(`[bitstream corruption] ${line}`)
          return
        }
        log.log({
          level: 'info',
          message: line,
        })
      })
  }

  #streamJpegToOutputStream(src: string, size: string = '640x480', signal?: AbortSignal, profile?: string, colorspace?: string, frameRate?: number, refs?: number, level?: string) {
    const ffmpegBinary = env.get('FFMPEG_BIN', 'ffmpeg')

    const ffmpegArgs = [
      '-loglevel',
      env.get('FFMPEG_DEBUG_LEVEL', 'warning'),
      '-loop',
      '1',
      // Hardware-accelerated decoding arguments
      //...this.#hardwareAcceleratedDecodingArguments,
      '-i',
      `${src}`,
      '-f',
      'lavfi',
      '-i',
      'anullsrc=r=48000:cl=stereo', // Synthetic audio source
      // Hardware-accelerated encoding arguments (no conflict now)
      //...this.#hardwareAcceleratedEncodingArguments,
      '-c:v',
      'libx264',
      '-preset',
      'ultrafast', // Ultrafast preset for low CPU on static placeholder
      '-profile:v',
      // When a detected profile is passed (single-process mode, 2nd+ cycle), match
      // the camera's profile so the VAAPI decoder's SPS doesn't change. e.g. "High"
      // → "high", "Constrained Baseline" → "constrained_baseline". Falls back to
      // "baseline" for the first cycle (before any format is detected) or in
      // restart-based mode where profile matching is not needed.
      profile ? profile.toLowerCase().replace(/ /g, '_') : 'baseline',
      // Empirical SPS matching for the matched-format placeholder (profile provided).
      // -refs 2: confirmed via VAAPI-encoded SRT output.
      // -level 3.2: ffprobe on the VAAPI SRT output confirmed level=32 (Level 3.2);
      //   3.1 was one step too low.
      // -tune zerolatency omitted here: it disables B-frames (has_b_frames=0), but
      //   the VAAPI output shows has_b_frames=1, matching what the camera's native
      //   H.264 produces. Without this tune, libx264 uses B-frames by default,
      //   keeping the placeholder SPS consistent with the camera side.
      // If dynamically-detected values are ever provided they take precedence.
      ...(refs !== undefined ? ['-refs', String(refs)] : profile !== undefined ? ['-refs', '2'] : []),
      ...(level !== undefined ? ['-level', level] : profile !== undefined ? ['-level', '3.2'] : []),
      '-r',
      frameRate ? String(Math.round(frameRate)) : '2', // match camera fps; default 2fps for static placeholder
      '-b:v',
      '100k', // Cap bitrate — static frame needs very little
      '-s',
      size,
      '-pix_fmt',
      'yuv420p',
      // colorspace flags intentionally omitted: profile/level are the only SPS
      // fields the VAAPI decoder's filter chain is sensitive to.

      // AAC Audio Stream (track 1)
      '-c:a:0',
      'aac',
      '-b:a:0',
      '128k', // Audio bitrate for AAC

      // Opus Audio Stream (track 2)
      '-c:a:1',
      'libopus',
      '-b:a:1',
      '128k', // Audio bitrate for Opus

      // Mapping inputs and outputs
      '-map',
      '0:v', // Map the video input to the H.264 video stream (image source)
      '-map',
      '1:a', // Map the synthetic audio source to the AAC stream
      '-map',
      '1:a', // Map the synthetic audio source again for Opus encoding

      '-f',
      'mpegts',
      '-listen',
      '0',
      '-use_wallclock_as_timestamps',
      '1',
      `unix:${this.#streamerPassthroughSock}`, // Send output to Unix socket
    ]

    this.#staticStreamLogger.info(
      `Spawning static input ffmpeg: ${ffmpegBinary} ${ffmpegArgs.join(' ')}`
    )
    this.#staticStreamer = execa(ffmpegBinary, ffmpegArgs, {
      stdio: 'pipe',
      reject: false,
      shell: false,
      signal,
    })
    this.#staticStreamer.catch((err) => {
      logger.error(err.message)
    })
    this.#staticStreamer.stdout!.on('data', (data) =>
      this.#onFFMpegCameraStreamOutput(data, this.#staticStreamLogger)
    )
    this.#staticStreamer.stderr!.on('data', (data) =>
      this.#onFFMpegCameraStreamOutput(data, this.#staticStreamLogger)
    )
    this.#staticStreamer.on('exit', async (code, es?: NodeJS.Signals) => {
      logger.info(`Static Input FFMpeg exited with code ${code}`)
      // During a planned output-streamer restart the static process may be killed
      // explicitly before the output streamer. Don't treat that as a fatal error.
      if ((signal && signal.aborted) || this.#outputStreamerIsRestarting) {
        return
      }
      if (code !== 0 && code !== 8 && es !== 'SIGABRT') {
        const res = await this.#streamer
        if (res) {
          logger.info(res.escapedCommand)
        }
        this.#gracefulExit(code || 0)
      } else {
        void this.#streamJpegToOutputStream(src, size, signal)
      }
    })
  }

  async #getRtspUrl(service: smartdevicemanagement_v1.Smartdevicemanagement, camera: Camera) {
    while ('string' !== typeof this.#rtspCameraStreamUrl) {
      let rtspUrl: any | undefined
      let streamExtensionToken: string | undefined
      let expiresAt: string | undefined
      try {
        const {
          data: { results },
        } = await service.enterprises.devices.executeCommand({
          name: camera.uid,
          requestBody: {
            command: 'sdm.devices.commands.CameraLiveStream.GenerateRtspStream',
          },
        })
        if (!results!.streamUrls || !results!.streamUrls.rtspUrl) {
          throw new Error('RTSP Stream URL not found')
        }
        if (!results!.streamExtensionToken) {
          throw new Error('No stream extension token found')
        }
        rtspUrl = results!.streamUrls.rtspUrl
        streamExtensionToken = results!.streamExtensionToken
        expiresAt = results!.expiresAt
      } catch (error) {
        if ((error as Error).message.includes('Rate limited')) {
          logger.warning(`Rate limited. Waiting ${NestmtxStream.#RATE_LIMIT_RETRY_MS / 1000}s before retrying`)
          await new Promise((r) => setTimeout(r, NestmtxStream.#RATE_LIMIT_RETRY_MS))
        } else {
          this.#gracefulExit(1)
        }
      }
      camera.streamExtensionToken = streamExtensionToken || null
      camera.expiresAt = DateTime.utc().plus({ minutes: 5 })
      if (expiresAt) {
        const expiresAtDateTime = DateTime.fromISO(expiresAt)
        if (expiresAtDateTime.isValid) {
          camera.expiresAt = expiresAtDateTime
        }
      }
      await camera.save()
      this.#rtspCameraStreamUrl = rtspUrl
    }
    return this.#rtspCameraStreamUrl
  }

  async #rtspStart(
    service: smartdevicemanagement_v1.Smartdevicemanagement,
    camera: Camera,
  ): Promise<void> {
    const ffmpegBinary = env.get('FFMPEG_BIN', 'ffmpeg')

    // Retry the stream-characteristics check with exponential backoff rather than
    // terminating after a fixed number of attempts. A camera that's rebooting or
    // temporarily unreachable would otherwise force a full process restart via PM3
    // on every occurrence. Backoff caps at 64 s so we don't hammer the Nest API.
    let rtspSrc: string = ''
    let attempt = 0
    while (true) {
      rtspSrc = await this.#getRtspUrl(service, camera)
      this.#cameraStreamLogger.info(
        `Getting RTSP stream characteristics for "${getHostnameFromRtspUrl(rtspSrc)}" (attempt ${attempt + 1})`
      )
      const getCharacteristicsAbortController = new AbortController()
      setTimeout(() => {
        getCharacteristicsAbortController.abort()
      }, NestmtxStream.#RTSP_CHARACTERISTICS_TIMEOUT_MS)
      try {
        await getRtspStreamCharacteristics(rtspSrc, getCharacteristicsAbortController.signal)
        break
      } catch (error) {
        this.#cameraStreamLogger.error(error.message)
        this.#rtspCameraStreamUrl = undefined
        const delay = getRtspCharacteristicsRetryDelayMs(attempt)
        this.#cameraStreamLogger.warning(
          `RTSP characteristics failed (attempt ${attempt + 1}), retrying in ${delay / 1000}s`
        )
        await new Promise<void>((r) => setTimeout(r, delay))
        attempt++
      }
    }

    const ffmpegArgs = buildRtspCameraFfmpegArgs({
      logLevel: env.get('FFMPEG_DEBUG_LEVEL', 'warning'),
      rtspSrc,
      outputPath: `unix:${this.#cameraPassthroughSock}`,
    })

    this.#connectingStreamAbortController.abort()
    this.#cameraStreamLogger.info(`Starting FFMpeg with RTSP stream`)
    if (!this.#persistentMode) {
      await this.#restartOutputStreamer(this.#isHwAccelEnabled)
    }
    this.#cameraStreamLogger.info(
      `Spawning RTSP camera ffmpeg: ${ffmpegBinary} ${ffmpegArgs.join(' ')}`
    )
    this.#cameraStreamer = execa(ffmpegBinary, ffmpegArgs, {
      stdio: 'pipe',
      reject: false,
      shell: false,
      signal: this.#abortController.signal,
    })
    this.#cameraStreamer.catch((err) => {
      this.#cameraStreamLogger.error(err.message)
    })
    this.#cameraStreamer.stdout!.on('data', (data) =>
      this.#onFFMpegCameraStreamOutput(data, this.#cameraStreamLogger)
    )
    this.#cameraStreamer.stderr!.on('data', (data) =>
      this.#onFFMpegCameraStreamOutput(data, this.#cameraStreamLogger)
    )
    this.#cameraStreamer.on('exit', async (code, es?: NodeJS.Signals) => {
      this.#cameraStreamLogger.info(`RTSP Camera FFMpeg exited with code ${code}`)
      if (code !== 0 && code !== 8 && es !== 'SIGABRT') {
        const res = this.#streamer ? await this.#streamer : undefined
        if (res) {
          this.#cameraStreamLogger.info(res.escapedCommand)
        }
        this.#gracefulExit(code || 0)
      } else {
        this.#connectingStreamAbortController = new AbortController()
        void this.#streamJpegToOutputStream(
          this.#connectingFilePath,
          camera.resolution || '640x480',
          this.#connectingStreamAbortController.signal
        )
        if (!this.#persistentMode) {
          void this.#restartOutputStreamer(false)
        }
        void this.#rtspStart(service, camera)
      }
    })
  }

  async #webrtcStart(service: smartdevicemanagement_v1.Smartdevicemanagement, camera: Camera) {
    if (!Array.isArray(this.#iceServers)) {
      throw new Error('Failed to get ICE servers')
    }

    // Diagnostic: dump format-matching state at session start so every subsequent
    // log entry can be cross-referenced against what was known at this point.
    this.#cameraStreamLogger.info(
      `webrtcStart: singleProcessMode=${this.#singleProcessMode} singleProcessFailed=${this.#singleProcessFailed} detectedSize=${this.#detectedCameraSize ?? 'none'} detectedProfile=${this.#detectedCameraProfile ?? 'none'} detectedColorspace=${this.#detectedCameraColorspace ?? 'none'} detectedFrameRate=${this.#detectedCameraFrameRate ?? 'none'} detectedRefs=${this.#detectedCameraRefs ?? 'none'} detectedLevel=${this.#detectedCameraLevel ?? 'none'}`
    )

    this.#connectingStreamAbortController = new AbortController()
    // In single-process mode, use the camera's detected resolution and H264 profile
    // so the VAAPI hardware decoder's filter chain never sees an SPS change when
    // camera data takes over. First cycle has no detection yet → default 1920x1080
    // baseline, which will trigger a reinit error and 55ms in-place recovery. Second
    // cycle onwards: placeholder matches camera → no reinit error.
    const placeholderSize = (this.#singleProcessMode && this.#detectedCameraSize)
      ? this.#detectedCameraSize
      : '1920x1080'
    const placeholderProfile = (this.#singleProcessMode && this.#detectedCameraProfile)
      ? this.#detectedCameraProfile
      : undefined
    const placeholderColorspace = (this.#singleProcessMode && this.#detectedCameraColorspace)
      ? this.#detectedCameraColorspace
      : undefined
    const placeholderFrameRate = (this.#singleProcessMode && this.#detectedCameraFrameRate)
      ? this.#detectedCameraFrameRate
      : undefined
    const placeholderRefs = (this.#singleProcessMode && this.#detectedCameraRefs !== undefined)
      ? this.#detectedCameraRefs
      : undefined
    const placeholderLevel = (this.#singleProcessMode && this.#detectedCameraLevel)
      ? this.#detectedCameraLevel
      : undefined
    this.#cameraStreamLogger.info(
      `webrtcStart: starting placeholder — size=${placeholderSize} profile=${placeholderProfile ?? 'baseline(default)'} colorspace=${placeholderColorspace ?? 'undetected'} fps=${placeholderFrameRate ?? '2(default)'} refs=${placeholderRefs ?? 'undetected'} level=${placeholderLevel ?? 'undetected'} (reason: ${this.#singleProcessMode && this.#detectedCameraSize ? 'matched detected camera format' : this.#singleProcessMode ? 'single-process mode but no detection yet' : 'restart-based mode or mode disabled'})`
    )
    this.#streamJpegToOutputStream(
      this.#connectingFilePath,
      placeholderSize,
      this.#connectingStreamAbortController.signal,
      placeholderProfile,
      placeholderColorspace,
      placeholderFrameRate,
      placeholderRefs,
      placeholderLevel,
    )

    const getPortOptions: PickPortOptions = {
      type: 'udp',
      ip: '0.0.0.0',
      reserveTimeout: 15,
      minPort: env.get('WEBRTC_RTP_MIN_PORT', 10000),
      maxPort: env.get('WEBRTC_RTP_MAX_PORT', 20000),
    }
    const ffmpegBinary = env.get('FFMPEG_BIN', 'ffmpeg')
    const audioPort = await pickPort(getPortOptions)
    const audioRTCPPort = await pickPort(getPortOptions)
    const videoPort = await pickPort(getPortOptions)
    const videoRTCPPort = await pickPort(getPortOptions)

    // Allocate the local SRT relay port for VAAPI restart-based mode only.
    // VAAPI requires a real network input to establish the hwaccel decode context;
    // pipe:3 MPEG-TS does not provide the stream-framing information the VA-API
    // driver needs. Other accelerators (CUDA, QSV, VideoToolbox) work fine with
    // pipe:3, so the relay is skipped for them. Single-process mode also skips it
    // (it keeps the existing Unix socket → pipe:3 path throughout).
    if (!this.#singleProcessMode && !this.#persistentMode && this.#isVaapiEnabled) {
      this.#cameraRelayPort = await pickPort({
        type: 'udp',
        ip: '0.0.0.0',
        reserveTimeout: 15,
        minPort: 20001,
        maxPort: 29999,
      })
      this.#cameraStreamLogger.info(`SRT relay: allocated local port ${this.#cameraRelayPort} for camera→output streamer relay`)
    }
    this.#udpSocket = createSocket('udp4')

    const pc = new RTCPeerConnection({
      bundlePolicy: 'max-bundle',
      codecs: {
        audio: [
          new RTCRtpCodecParameters({
            mimeType: 'audio/opus',
            clockRate: 48000,
            channels: 2,
          }),
        ],
        video: [
          new RTCRtpCodecParameters({
            mimeType: 'video/H264',
            clockRate: 90000,
            rtcpFeedback: [
              { type: 'transport-cc' },
              { type: 'ccm', parameter: 'fir' },
              { type: 'nack' },
              { type: 'nack', parameter: 'pli' },
              { type: 'goog-remb' },
            ],
            parameters: 'level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=42e01f',
          }),
        ],
      },
      iceServers: this.#iceServers,
      iceAdditionalHostAddresses: this.#additionalHostAddresses,
      iceTransportPolicy: 'all',
    })

    pc.addEventListener('connectionstatechange', () => {
      switch (pc.connectionState) {
        case 'new':
        case 'connecting':
          this.#cameraStreamLogger.info('WebRTC Peer connection state: connecting')
          break
        case 'connected':
          this.#cameraStreamLogger.info('WebRTC Peer connection state: connected')
          break
        case 'disconnected':
        case 'closed':
        case 'failed':
          this.#cameraStreamLogger.warning('WebRTC Peer connection state: disconnected')
          break
        default:
          this.#cameraStreamLogger.warning('WebRTC Peer connection state: unknown')
          break
      }
    })

    const peerConnectedAbortController = new AbortController()

    const peerConnected = new Promise<void>((resolve, reject) => {
      let settled = false
      let connectionTimeoutId: NodeJS.Timeout
      const settle = (fn: () => void) => {
        if (settled) return
        settled = true
        clearTimeout(connectionTimeoutId)
        fn()
      }
      const onConnectionStateChange = () => {
        switch (pc.connectionState) {
          case 'connected':
            pc.removeEventListener('connectionstatechange', onConnectionStateChange)
            settle(resolve)
            break
          case 'disconnected':
          case 'closed':
          case 'failed':
            pc.removeEventListener('connectionstatechange', onConnectionStateChange)
            settle(() => reject(new Error('WebRTC Peer connection failed')))
            break
          default:
            break
        }
      }
      pc.addEventListener('connectionstatechange', onConnectionStateChange)
      // Graceful shutdown: abort signal resolves cleanly (not an error).
      peerConnectedAbortController.signal.addEventListener('abort', () => settle(resolve))
      connectionTimeoutId = setTimeout(
        () => settle(() => reject(new Error(`WebRTC peer connection timed out after ${NestmtxStream.#WEBRTC_CONNECTION_TIMEOUT_MS}ms`))),
        NestmtxStream.#WEBRTC_CONNECTION_TIMEOUT_MS
      )
    })

    peerConnected
      .then(() => {
        this.#cameraStreamLogger.info('WebRTC Peer connection established')
      })
      .catch((err: Error) => {
        this.#cameraStreamLogger.error(`WebRTC peer connection failed: ${err.message}`)
        this.#gracefulExit(1)
      })

    pc.addEventListener('icecandidateerror', (event) => {
      const e = new IceCandidateError(
        event.address,
        event.errorCode,
        event.errorText,
        event.port,
        event.url
      )
      this.#cameraStreamLogger.error(e)
    })

    const videoRtpBus = new EventEmitter({
      captureRejections: true,
    })

    const audioRtpBus = new EventEmitter({
      captureRejections: true,
    })

    const rtpPromiseAbortController = new AbortController()

    const videoRtpSending = new Promise<void>((resolve, reject) => {
      videoRtpBus.once('sent', () => {
        this.#cameraStreamLogger.info('Video Stream Started')
        return resolve(void 0)
      })
      videoRtpBus.once('error', (error: Error) => reject(error))
      rtpPromiseAbortController.signal.addEventListener('abort', () => resolve(void 0))
    })

    const audioRtpSending = new Promise<void>((resolve, reject) => {
      audioRtpBus.once('sent', () => {
        this.#cameraStreamLogger.info('Audio Stream Started')
        resolve(void 0)
      })
      audioRtpBus.once('error', (error: Error) => reject(error))
      rtpPromiseAbortController.signal.addEventListener('abort', () => resolve(void 0))
    })

    // Reset per-connection state so stale values from a previous WebRTC session
    // don't leak into PLI sends for the new session's receiver/SSRC.
    this.#videoReceiver = undefined
    this.#videoSsrc = undefined

    pc.addEventListener('track', (event: RTCTrackEvent) => {
      if (event.track.kind === 'video') {
        this.#videoReceiver = event.receiver
      }
      // Sample SSRC from the first N video packets rather than just the very first.
      // If the first RTP packet is dropped by the network, this ensures PLI targeting
      // is populated before the stream stalls waiting for an IDR frame.
      let videoSsrcSamplesLeft = NestmtxStream.#SSRC_SAMPLE_PACKETS
      const { unSubscribe } = event.track.onReceiveRtp.subscribe((rtp) => {
        if (event.track.kind === 'video' && videoSsrcSamplesLeft > 0) {
          videoSsrcSamplesLeft--
          this.#videoSsrc = rtp.header.ssrc
        }
        switch (event.track.kind) {
          case 'video':
            this.#udpSocket!.send(rtp.serialize(), videoPort, '0.0.0.0', (error, _bytes) => {
              if (error) {
                this.#cameraStreamLogger.error(error)
                return
              }
              // logger.debug(`Sent ${bytes} bytes of video data to 0.0.0.0:${videoPort}`)
              videoRtpBus.emit('sent')
            })
            break

          case 'audio':
            this.#udpSocket!.send(rtp.serialize(), audioPort, '0.0.0.0', (error, _bytes) => {
              if (error) {
                this.#cameraStreamLogger.error(error)
                return
              }
              // logger.debug(`Sent ${bytes} bytes of audio data to 0.0.0.0:${audioPort}`)
              audioRtpBus.emit('sent')
            })
            break

          default:
            break
        }
      })
      rtpPromiseAbortController.signal.addEventListener('abort', () => unSubscribe())
    })

    try {
      pc.addTransceiver('audio', { direction: 'recvonly' })
    } catch (error) {
      throw new Error(`Failed to add audio transceiver: ${error.message}`)
    }

    try {
      pc.addTransceiver('video', { direction: 'recvonly' })
    } catch (error) {
      throw new Error(`Failed to add video transceiver: ${error.message}`)
    }

    // Add a data channel to include the application m line in SDP
    pc.createDataChannel('dataSendChannel', { id: 1 })

    const offer = await pc.createOffer()
    await pc.setLocalDescription(offer)

    const {
      data: { results },
    } = await service.enterprises.devices.executeCommand({
      name: camera.uid,
      requestBody: {
        command: 'sdm.devices.commands.CameraLiveStream.GenerateWebRtcStream',
        params: {
          offerSdp: offer.sdp,
        },
      },
    })

    if (!results!.answerSdp) {
      throw new Error('WebRTC Answer SDP not found')
    }
    if (!results!.mediaSessionId) {
      throw new Error('Media Session ID not found')
    }

    camera.streamExtensionToken = results!.mediaSessionId
    camera.expiresAt = DateTime.utc().plus({ minutes: 5 })
    if (results!.expiresAt) {
      const expiresAt = DateTime.fromISO(results!.expiresAt)
      if (expiresAt.isValid) {
        camera.expiresAt = expiresAt
      }
    }
    await camera.save()

    await pc.setRemoteDescription({
      type: 'answer',
      sdp: results!.answerSdp,
    })

    await Promise.all([videoRtpSending, audioRtpSending])

    const sdp = `v=0
o=- 0 0 IN IP4 127.0.0.1
s=FFmpeg RTP Stream
c=IN IP4 127.0.0.1
t=0 0

m=video ${videoPort} RTP/AVP 97
a=rtpmap:97 H264/90000
a=recvonly
a=rtcp:${videoRTCPPort}

m=audio ${audioPort} RTP/AVP 96
a=rtpmap:96 OPUS/48000/2
a=recvonly
a=rtcp:${audioRTCPPort}
`

    await writeFile(this.#streamerFFMpegInputSdp, sdp)
    this.#connectingStreamAbortController.abort()
    if (this.#persistentMode) {
      // Wait for the placeholder to fully exit before camera ffmpeg starts writing
      // to camera.sock. Without this, both could briefly write to pipe:3 simultaneously,
      // producing interleaved h264 that the output streamer (copy mode) cannot parse.
      if (this.#staticStreamer && this.#staticStreamer.exitCode === null) {
        const ss = this.#staticStreamer
        await new Promise<void>((resolve) => {
          const timeout = setTimeout(() => {
            this.#cameraStreamLogger.warning(
              `persistent mode: placeholder (pid ${ss.pid}) did not exit within ${NestmtxStream.#PLACEHOLDER_EXIT_WAIT_MS}ms — proceeding anyway; pipe:3 may briefly receive interleaved data`
            )
            resolve()
          }, NestmtxStream.#PLACEHOLDER_EXIT_WAIT_MS)
          ss.once('exit', () => { clearTimeout(timeout); resolve() })
        })
      }
    } else if (this.#singleProcessMode) {
      this.#singleProcessCameraActive = true
      this.#detectedCameraForCurrentCycleAt = 0
    }
    this.#cameraStreamLogger.info(`Starting FFMpeg with WebRTC stream`)

    // Output destination: SRT relay (restart-based mode) or Unix socket (single-process/persistent).
    // In restart-based mode, camera ffmpeg acts as an SRT listener so the VAAPI output
    // streamer can connect to it as a regular network input, enabling proper hwaccel
    // decode context setup. In single-process/persistent mode the output streamer stays
    // running and reads from pipe:3, so the Unix socket path is kept unchanged.
    const cameraOutputPath = this.#cameraRelayPort !== undefined
      ? `srt://127.0.0.1:${this.#cameraRelayPort}?mode=listener&pkt_size=1316`
      : `unix:${this.#cameraPassthroughSock}`

    const ffmpegArgs = buildCameraFfmpegArgs({
      logLevel: env.get('FFMPEG_DEBUG_LEVEL', 'warning'),
      sdpPath: this.#streamerFFMpegInputSdp,
      outputPath: cameraOutputPath,
      persistentMode: this.#persistentMode,
    })

    // Spawn camera ffmpeg BEFORE the output-streamer restart so it opens the
    // UDP ports immediately. SPS/PPS NAL units are sent by the WebRTC peer at
    // the very start of the H264 stream. Waiting until after the restart (~1s)
    // means they're gone and the H264 parser can never find frame boundaries,
    // producing "non-existing PPS referenced" errors for the entire session.
    // In SRT relay mode, camera ffmpeg starts as the SRT listener; the output
    // streamer connects to it after the restart completes (~400-800ms later).
    this.#cameraStreamLogger.info(
      `Spawning WebRTC camera ffmpeg: ${ffmpegBinary} ${ffmpegArgs.join(' ')}`
    )
    this.#cameraStreamer = execa(ffmpegBinary, ffmpegArgs, {
      stdio: 'pipe',
      reject: false,
      shell: false,
      signal: this.#abortController.signal,
    })

    this.#cameraStreamer.catch((err) => {
      logger.error(err.message)
    })
    this.#cameraStreamer.stdout!.on('data', (data) =>
      this.#onFFMpegCameraStreamOutput(data, this.#cameraStreamLogger)
    )
    this.#cameraStreamer.stderr!.on('data', (data) =>
      this.#onFFMpegCameraStreamOutput(data, this.#cameraStreamLogger)
    )
    this.#cameraStreamer.on('exit', async (code, es?: NodeJS.Signals) => {
      this.#cameraStreamLogger.info(`WebRTC Camera FFMpeg exited with code ${code}`)
      const isUnexpected = code !== 0 && code !== 8 && es !== 'SIGABRT'
      if (isUnexpected && this.#persistentMode) {
        // Persistent mode: unexpected camera crash — restart the output streamer
        // in copy mode (to unfreeze pipe:3) and re-enter the placeholder→camera cycle.
        this.#cameraStreamLogger.warning(
          `persistent mode: camera crashed unexpectedly (code=${code}) — restarting output streamer and reconnecting`
        )
        await this.#restartOutputStreamer(false)
        void this.#webrtcStart(service, camera)
      } else if (isUnexpected && this.#singleProcessMode) {
        // Unexpected crash in single-process mode — recover in-place rather than
        // tearing down the whole process. A camera crash (bitstream corruption,
        // exit code 183, etc.) leaves the VAAPI output streamer frozen: it's still
        // running but blocking on pipe:3 with no writer. The recovery is identical
        // to reinit-failure recovery — kill and respawn the frozen output streamer,
        // then re-enter the placeholder→camera cycle. Camera crashes count toward
        // the same failure budget as reinit errors; after 3 total, single-process
        // mode is disabled for the session.
        this.#singleProcessReinitCount++
        this.#cameraStreamLogger.warning(
          `single-process mode: camera crashed unexpectedly (code=${code}) — recovering in-place, failure #${this.#singleProcessReinitCount} (detectedSize=${this.#detectedCameraSize ?? 'none'} detectedProfile=${this.#detectedCameraProfile ?? 'none'} detectedColorspace=${this.#detectedCameraColorspace ?? 'none'} detectedRefs=${this.#detectedCameraRefs ?? 'none'} detectedLevel=${this.#detectedCameraLevel ?? 'none'})`
        )
        if (this.#singleProcessReinitCount >= 3) {
          this.#singleProcessFailed = true
          this.#cameraStreamLogger.warning(
            `single-process mode: ${this.#singleProcessReinitCount} consecutive failures — disabling for this session, subsequent cycles will use restart-based mode`
          )
        }
        this.#singleProcessCameraActive = false
        // Restart the output streamer to unfreeze it. #singleProcessMode now
        // returns false if #singleProcessFailed was just set above, so this
        // automatically falls back to software encoding on the 3rd failure.
        await this.#restartOutputStreamer(this.#singleProcessMode)
        this.#cameraStreamLogger.info(
          `single-process mode: output streamer restarted (pid=${this.#streamer?.pid}) after camera crash`
        )
        void this.#webrtcStart(service, camera)
      } else if (isUnexpected) {
        // Unexpected exit in restart-based mode — full teardown, PM3 will restart.
        const res = this.#streamer ? await this.#streamer : undefined
        if (res) {
          this.#cameraStreamLogger.info(res.escapedCommand)
        }
        this.#gracefulExit(code || 0)
      } else {
        // Expected exit (code 0, 8, or SIGABRT from a deliberate kill).
        if (this.#persistentMode) {
          // Persistent mode: output streamer stays in copy mode throughout.
          // The next #webrtcStart will restart the placeholder, which writes to
          // streamer.sock → pipe:3. The output streamer keeps running uninterrupted.
          this.#cameraStreamLogger.info(
            `persistent mode: camera exited, output streamer pid=${this.#streamer?.pid} stays running`
          )
        } else if (!this.#singleProcessMode) {
          // Restart-based mode: restart back to software before starting a new
          // placeholder phase (the next #webrtcStart call starts a new static input
          // and will restart to VAAPI again when the camera reconnects).
          if (this.#cameraRelayPort !== undefined) {
            // During the SRT relay camera phase, packets don't flow through Node,
            // so #lastThirtyPacketCounts has been filling up with zeros. Clear it
            // now so the stall detector doesn't immediately fire when returning to
            // the placeholder phase (where packets flow through fd3 again).
            this.#lastThirtyPacketCounts = []
            this.#stalled = false
          }
          void this.#restartOutputStreamer(false)
        } else {
          // Single-process mode: output streamer stays in VAAPI mode throughout.
          // The new static input will be fed as H264 to the same running VAAPI
          // output streamer; the VAAPI decoder sees another SPS transition (live
          // camera → placeholder H264) which is also part of what we're testing.
          this.#singleProcessCameraActive = false
          // A clean camera exit means this session ran to completion. Reset the
          // consecutive-failure counter so the next cycle gets its full 3 attempts.
          const hadFailuresThisSession = this.#singleProcessReinitCount > 0
          if (hadFailuresThisSession) {
            this.#cameraStreamLogger.info(
              `single-process mode: camera session completed cleanly — resetting reinit count to 0 (was ${this.#singleProcessReinitCount})`
            )
            this.#singleProcessReinitCount = 0
          }
          // If the session completed with zero reinit failures, the prior failures
          // were transient — re-enable single-process mode so the next reconnect
          // can use VAAPI again rather than staying in software indefinitely.
          if (this.#singleProcessFailed && !hadFailuresThisSession) {
            this.#cameraStreamLogger.info(
              `single-process mode: clean session with no reinit errors — re-enabling for next reconnect`
            )
            this.#singleProcessFailed = false
          }
          this.#cameraStreamLogger.info(
            `single-process mode: camera exited, output streamer pid=${this.#streamer?.pid} stays running`
          )
        }
        void this.#webrtcStart(service, camera)
      }
    })

    // Diagnostic: log process state at T+2s so we can see whether camera ffmpeg
    // is alive, already exited, or just silent at that point in the timeline.
    const diagnosticRef = this.#cameraStreamer
    setTimeout(() => {
      this.#cameraStreamLogger.info(
        `[diag T+${NestmtxStream.#DIAGNOSTIC_DELAY_MS}ms] cameraStreamer pid=${diagnosticRef.pid} exitCode=${diagnosticRef.exitCode ?? 'null(running)'} | outputStreamer pid=${this.#streamer?.pid} exitCode=${this.#streamer?.exitCode ?? 'null(running)'}`
      )
    }, NestmtxStream.#DIAGNOSTIC_DELAY_MS)

    if (this.#persistentMode) {
      // Persistent mode: output streamer stays in copy mode — no restart needed.
      // Just send a PLI so the camera delivers a fresh IDR for the new data that
      // will flow into pipe:3 now that the placeholder has stopped writing.
      this.#cameraStreamLogger.info(
        `persistent mode: skipping restart, output streamer pid=${this.#streamer?.pid} stays running`
      )
      if (this.#videoReceiver !== undefined && this.#videoSsrc !== undefined) {
        try {
          await this.#videoReceiver.sendRtcpPLI(this.#videoSsrc)
        } catch (err: any) {
          this.#cameraStreamLogger.warning(`Persistent PLI send failed: ${err.message}`)
        }
      }
    } else if (!this.#singleProcessMode) {
      // Restart-based mode (default): kill the software output streamer and
      // spawn a new VAAPI one. Two PLIs bridge the ~400ms kill/respawn gap —
      // the early one puts the camera's keyframe round-trip in flight while the
      // restart runs; the post-restart one ensures the new output streamer gets
      // a clean IDR once it's ready to receive data.
      if (this.#videoReceiver !== undefined && this.#videoSsrc !== undefined) {
        this.#cameraStreamLogger.info(
          `Sending early PLI before restart (video SSRC=${this.#videoSsrc})`
        )
        this.#videoReceiver.sendRtcpPLI(this.#videoSsrc).catch((err: any) => {
          this.#cameraStreamLogger.warning(`Early PLI send failed: ${err.message}`)
        })
      } else {
        this.#cameraStreamLogger.warning(
          `Early PLI skipped: videoReceiver=${this.#videoReceiver !== undefined}, videoSsrc=${this.#videoSsrc}`
        )
      }

      await this.#restartOutputStreamer(this.#isHwAccelEnabled)

      if (this.#videoReceiver !== undefined && this.#videoSsrc !== undefined) {
        this.#cameraStreamLogger.info(
          `Sending post-restart PLI (video SSRC=${this.#videoSsrc})`
        )
        try {
          await this.#videoReceiver.sendRtcpPLI(this.#videoSsrc)
        } catch (err: any) {
          this.#cameraStreamLogger.warning(`Post-restart PLI send failed: ${err.message}`)
        }
      } else {
        this.#cameraStreamLogger.warning(
          `Post-restart PLI skipped: videoReceiver=${this.#videoReceiver !== undefined}, videoSsrc=${this.#videoSsrc}`
        )
      }
    } else {
      // Single-process investigation mode: output streamer is already running in
      // VAAPI mode with software decode, so it handles the SPS change when camera
      // data arrives on pipe:3. Do not restart it — just send a PLI so the camera
      // delivers a fresh IDR + SPS/PPS for the new output streamer to start from.
      this.#cameraStreamLogger.info(
        `single-process mode: skipping restart, output streamer pid=${this.#streamer?.pid} stays in VAAPI mode`
      )
      if (this.#videoReceiver !== undefined && this.#videoSsrc !== undefined) {
        this.#cameraStreamLogger.info(
          `Sending PLI for fresh keyframe (single-process, video SSRC=${this.#videoSsrc})`
        )
        try {
          await this.#videoReceiver.sendRtcpPLI(this.#videoSsrc)
        } catch (err: any) {
          this.#cameraStreamLogger.warning(`Single-process PLI send failed: ${err.message}`)
        }
      } else {
        this.#cameraStreamLogger.warning(
          `Single-process PLI skipped: videoReceiver=${this.#videoReceiver !== undefined}, videoSsrc=${this.#videoSsrc}`
        )
      }

    }
  }

  #gracefulExit(code: number = 0) {
    logger.info(
      `gracefulExit(${code}): cameraStreamer pid=${this.#cameraStreamer?.pid} exitCode=${this.#cameraStreamer?.exitCode ?? 'null'}, streamer pid=${this.#streamer?.pid} exitCode=${this.#streamer?.exitCode ?? 'null'}`
    )
    if (this.#streamer) {
      this.#streamer.kill('SIGKILL')
    }
    if (this.#staticStreamer) {
      this.#staticStreamer.kill('SIGKILL')
    }
    if (this.#cameraStreamer) {
      this.#cameraStreamer.kill('SIGKILL')
    }
    if (this.#streamerSocket) {
      this.#streamerSocket.close()
    }
    if (this.#cameraSocket) {
      this.#cameraSocket.close()
    }
    if (this.#udpSocket) {
      this.#udpSocket.close()
    }
    execa('rm', [this.#streamerPassthroughSock, this.#streamerFFMpegInputSdp])
      .catch(() => {})
      .finally(() => {
        process.exit(code)
      })
  }
}
