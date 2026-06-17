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
  #detectedCameraSize?: string    // e.g. "1152x864"
  #detectedCameraProfile?: string // e.g. "High", "Main"
  #singleProcessCameraActive: boolean = false
  // Guards against overwriting a correct detection with a wrong one. ffmpeg logs two
  // "Video: h264 (...)" lines per session: the input-side description (camera's real
  // profile, e.g. Main) and the encoder's output-side description (~30ms later, always
  // High because h264_vaapi chose High for its own output). Both lines match the
  // detection regex, so without this flag the correct profile gets overwritten.
  // Cleared when the camera phase starts; set on the first successful match.
  #detectedCameraForCurrentCycle: boolean = false

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
    if (this.#singleProcessMode) {
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
      if (
        !this.#stalled &&
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
        if (firstWrite) {
          firstWrite = false
          this.#outputStreamLogger.info(`First data from static input reaching pipe:3`)
        }
        this.#packetsToOutputCount += 1
        this.#stalled = false
        // @ts-expect-error - this is correct
        this.#streamer.stdio[3].write(raw)
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
        if (firstWrite) {
          firstWrite = false
          this.#outputStreamLogger.info(
            `First data from camera input reaching pipe:3 of output streamer pid=${this.#streamer.pid}`
          )
        }
        this.#packetsToOutputCount += 1
        this.#stalled = false
        // @ts-expect-error - this is correct
        this.#streamer.stdio[3].write(raw)
      }
    })
    socket.on('end', () => logger.info(`camera.sock: client disconnected (end)`))
    socket.on('close', () => logger.info(`camera.sock: client disconnected (close)`))
    socket.on('error', (error) => {
      logger.error(`camera.sock client error: ${error.message}`)
    })
  }

  #startOutputStreamer(useVaapi: boolean = false) {
    const ffmpegBinary = env.get('FFMPEG_BIN', 'ffmpeg')
    const accelDevice = env.get('FFMPEG_HW_ACCELERATOR_DEVICE', '/dev/dri/renderD128')

    // In single-process mode we need ffmpeg to emit stream-info lines so we can
    // detect the camera's resolution and H264 profile. Those lines are logged at
    // ffmpeg's "info" level — they are suppressed by the default "warning" level.
    // Boost to "info" only for this path; the extra verbosity is acceptable for
    // an experimental mode and the detection logic filters what it cares about.
    const logLevel = (useVaapi && this.#singleProcessMode)
      ? 'info'
      : env.get('FFMPEG_DEBUG_LEVEL', 'warning')

    const ffmpegArgs: string[] = [
      '-loglevel',
      logLevel,
      '-fflags',
      '+discardcorrupt+genpts',
      '-avoid_negative_ts',
      'make_zero',
    ]

    if (useVaapi) {
      // init_hw_device + filter_hw_device is the explicit, portable VAAPI setup.
      // -vaapi_device is a shorthand that can fail to propagate to the filtergraph
      // on some driver versions; this form is unambiguous.
      ffmpegArgs.push(
        '-init_hw_device', `vaapi=va:${accelDevice}`,
        '-filter_hw_device', 'va',
        '-hwaccel', 'vaapi',
        '-hwaccel_output_format', 'vaapi',
        '-hwaccel_device', 'va',
        '-extra_hw_frames', '64',
      )
    }

    // Applied regardless of encoder: on severely corrupted/lossy RTP input the
    // decoder (VAAPI hardware or software libx264) can spin producing rapid
    // "Invalid data found" errors rather than dropping bad frames and continuing.
    // ignore_err tells the decoder to absorb errors silently, preventing the
    // error loop that was observed to delay SIGTERM response. This covers both the
    // primary VAAPI path and the software fallback NestMTX switches to on a stall.
    ffmpegArgs.push('-err_detect', 'ignore_err')

    ffmpegArgs.push('-i', 'pipe:3')

    if (useVaapi) {
      // scale_vaapi keeps the frame on the GPU and ensures NV12 format, which
      // h264_vaapi requires. Replaces format=nv12,hwupload which would download
      // to CPU then re-upload — that was what caused the swscaler warning.
      ffmpegArgs.push(
        '-vf',
        'scale_vaapi=format=nv12',
        '-c:v',
        'h264_vaapi',
        '-b:v',
        '2M',
        '-maxrate',
        '2M',
      )
    } else {
      ffmpegArgs.push(
        '-c:v',
        'libx264',
        '-preset',
        'veryfast',
        '-tune',
        'zerolatency',
        '-vf',
        'fps=15',
        '-b:v',
        '2M',
        '-maxrate',
        '2M'
      )
    }

    ffmpegArgs.push(
      // AAC Audio Stream (track 1)
      '-c:a:0',
      'aac',
      '-b:a:0',
      '128k',

      // Opus Audio Stream (track 2)
      '-c:a:1',
      'libopus',
      '-b:a:1',
      '128k',

      '-map',
      '0:v:0',
      '-map',
      '0:a:0',
      '-map',
      '0:a:1',

      '-f',
      'mpegts',

      this.#destination,
    )

    this.#outputStreamLogger.info(
      `Spawning output streamer: ${ffmpegBinary} ${ffmpegArgs.join(' ')}`
    )
    this.#streamer = execa(ffmpegBinary, ffmpegArgs, {
      stdio: ['pipe', 'pipe', 'pipe', 'pipe'],
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
          if (this.#singleProcessMode && this.#singleProcessCameraActive && !this.#detectedCameraForCurrentCycle) {
            const m = line.match(/Video: h264 \(([^)]+)\).*?(\d{3,4}x\d{3,4})/)
            if (m) {
              this.#detectedCameraProfile = m[1]
              this.#detectedCameraSize = m[2]
              this.#detectedCameraForCurrentCycle = true
              this.#outputStreamLogger.info(
                `single-process mode: stored camera format — size=${this.#detectedCameraSize} profile=${this.#detectedCameraProfile} (from line: ${line.substring(0, 120)})`
              )
            }
          }
        })
    })
    // Node.js backs stdio[3] with a net.Socket internally. When the process is
    // killed during a restart the socket emits 'error' (ECONNRESET / EPIPE).
    // Without a listener that becomes an uncaught exception and crashes the process.
    // @ts-expect-error - stdio[3] is a net.Socket at runtime
    this.#streamer.stdio[3].on('error', (err: Error) => {
      if (!this.#outputStreamerIsRestarting) {
        this.#outputStreamLogger.error(`Output pipe fd3 error: ${err.message}`)
      }
    })
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
            `single-process mode: ${this.#singleProcessReinitCount} consecutive reinit failures — disabling for this session, subsequent cycles will use restart-based mode (detectedSize=${this.#detectedCameraSize ?? 'none'} detectedProfile=${this.#detectedCameraProfile ?? 'none'})`
          )
        } else {
          logger.warning(
            `single-process mode: reinit failure #${this.#singleProcessReinitCount} — restarting output streamer in-place (detectedSize=${this.#detectedCameraSize ?? 'none'} detectedProfile=${this.#detectedCameraProfile ?? 'none'})`
          )
        }
        // Restart with write-gating so camera data doesn't stream into a half-dead pipe.
        this.#outputStreamerIsRestarting = true
        this.#startOutputStreamer(true)
        this.#outputStreamerIsRestarting = false
        logger.info(
          `single-process mode: output streamer restarted (pid=${this.#streamer?.pid}) after reinit failure`
        )
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

  async #restartOutputStreamer(useVaapi: boolean) {
    // Kill the static placeholder explicitly before touching the output streamer.
    // With shell:false the execa kill now hits ffmpeg directly (no shell orphan),
    // but we still need to wait for the process to actually exit so no lingering
    // data reaches pipe:3 of the new output streamer.
    // Only done for the VAAPI transition (useVaapi=true); when switching back to
    // software, #webrtcStart creates a new #staticStreamer concurrently and we
    // must not kill it.
    if (useVaapi && this.#staticStreamer) {
      const ss = this.#staticStreamer
      if (ss.exitCode === null) {
        logger.info(`restartOutputStreamer: killing static streamer (pid ${ss.pid})`)
        await new Promise<void>((resolve) => {
          const timeout = setTimeout(() => {
            logger.info(`restartOutputStreamer: static streamer kill timed out`)
            resolve()
          }, 2000)
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
      logger.info(`restartOutputStreamer: killing output streamer, useVaapi=${useVaapi}`)
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
            `restartOutputStreamer: output streamer SIGTERM unresponsive after 1.5s, escalating to SIGKILL (pid ${streamer.pid})`
          )
          try {
            streamer.kill('SIGKILL')
          } catch {}
        }, 1500)

        // Hard safety valve in case SIGKILL doesn't produce an exit event (zombie).
        const hardTimeout = setTimeout(() => {
          clearTimeout(sigkillTimeout)
          logger.info(
            `restartOutputStreamer: output streamer hard kill timeout (pid ${streamer.pid})`
          )
          resolve()
        }, 3000)

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
      this.#outputStreamerIsRestarting = false
    }
    logger.info(`restartOutputStreamer: starting new output streamer (useVaapi=${useVaapi})`)
    this.#startOutputStreamer(useVaapi)
    logger.info(`restartOutputStreamer: new output streamer pid=${this.#streamer?.pid}`)
  }

  // ^(.*)\s+VBV\s+underflow\s+\(frame\s+\d+,\s+\-?\d+\s+bits\)$

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
          }, 10000)
          const duration = this.#lastUnderflowWarningAt.diff(this.#firstUnderflowWarningAt)
          if (duration.as('seconds') > 10) {
            log.warning('VBV underflow detected for more than 10 seconds. Sending "stall" signal')
            this.#bus.emit('stall')
          }
        }
        if (line.includes('Last message repeated 3')) {
          return
        }
        log.log({
          level: 'info',
          message: line,
        })
      })
  }

  #streamJpegToOutputStream(src: string, size: string = '640x480', signal?: AbortSignal, profile?: string) {
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
      '-tune',
      'zerolatency',
      '-r',
      '2', // 2fps is sufficient for a static placeholder image
      '-b:v',
      '100k', // Cap bitrate — static frame needs very little
      '-s',
      size,
      '-pix_fmt',
      'yuv420p',

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
          logger.warning('Rate limited. Waiting 30 seconds before retrying')
          await new Promise((r) => setTimeout(r, 30000))
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
    depth: number = 0
  ): Promise<void> {
    const ffmpegBinary = env.get('FFMPEG_BIN', 'ffmpeg')
    const rtspSrc = await this.#getRtspUrl(service, camera)
    this.#cameraStreamLogger.info(
      `Getting RTSP stream characteristics for "${getHostnameFromRtspUrl(rtspSrc)}"`
    )
    const getCharacteristicsAbortController = new AbortController()
    setTimeout(() => {
      getCharacteristicsAbortController.abort()
    }, 30000)
    try {
      await getRtspStreamCharacteristics(
        rtspSrc,
        getCharacteristicsAbortController.signal
      )
    } catch (error) {
      this.#cameraStreamLogger.error(error.message)
      this.#rtspCameraStreamUrl = undefined
      if (depth > 5) {
        return this.#gracefulExit(1)
      } else {
        return this.#rtspStart(service, camera, depth + 1)
      }
    }
    const ffmpegArgs: string[] = [
      '-loglevel',
      env.get('FFMPEG_DEBUG_LEVEL', 'warning'), // Suppress most log messages, only show warnings
      '-fflags',
      '+discardcorrupt+nobuffer', // Ignore corrupted frames and minimize buffering

      // Limit avformat_find_stream_info to 100ms. RTSP DESCRIBE already provides codec
      // info; the 5s default analysis wait causes the same race as the WebRTC case.
      '-analyzeduration',
      '100000', // 100ms in microseconds

      // No hwaccel decoding args: same reasoning as the WebRTC camera — -c:v copy
      // passes the H264 bitstream through without decoding, so VAAPI decoding init
      // is both unnecessary and conflicting with the VAAPI output streamer.

      // Rate-limit reading to the stream's own timestamps, absorbing CDN burst delivery
      '-re',

      '-i',
      rtspSrc,

      // Retry options for network issues
      '-rtsp_transport',
      'udp', // Use UDP to reduce latency

      // Pass through video without re-encoding
      '-c:v',
      'copy',

      // AAC Audio Stream
      '-c:a:0',
      'aac',
      '-b:a:0',
      '128k', // Audio bitrate for AAC

      // Opus Audio Stream
      '-c:a:1',
      'libopus',
      '-b:a:1',
      '128k', // Audio bitrate for Opus

      // Mapping inputs and outputs
      '-map',
      '0:v', // Map the video input to the H.264 video stream
      '-map',
      '0:a', // Map the original AAC audio to the first audio track
      '-map',
      '0:a', // Map the original audio again for Opus encoding

      '-f',
      'mpegts',
      '-listen',
      '0',

      // Limit threads before the output URL (trailing options after the URL are ignored)
      '-threads',
      '1',

      `unix:${this.#cameraPassthroughSock}`,
    ]

    this.#connectingStreamAbortController.abort()
    this.#cameraStreamLogger.info(`Starting FFMpeg with RTSP stream`)
    await this.#restartOutputStreamer(this.#isVaapiEnabled)
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
        void this.#restartOutputStreamer(false)
        void this.#rtspStart(service, camera, 0)
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
      `webrtcStart: singleProcessMode=${this.#singleProcessMode} singleProcessFailed=${this.#singleProcessFailed} detectedSize=${this.#detectedCameraSize ?? 'none'} detectedProfile=${this.#detectedCameraProfile ?? 'none'}`
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
    this.#cameraStreamLogger.info(
      `webrtcStart: starting placeholder — size=${placeholderSize}${placeholderProfile ? ` profile=${placeholderProfile}` : ' profile=baseline(default)'} (reason: ${this.#singleProcessMode && this.#detectedCameraSize ? 'matched detected camera format' : this.#singleProcessMode ? 'single-process mode but no detection yet' : 'restart-based mode or mode disabled'})`
    )
    this.#streamJpegToOutputStream(
      this.#connectingFilePath,
      placeholderSize,
      this.#connectingStreamAbortController.signal,
      placeholderProfile,
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
      const onConnectionStateChange = () => {
        switch (pc.connectionState) {
          case 'connected':
            pc.removeEventListener('connectionstatechange', onConnectionStateChange)
            return resolve(void 0)
          case 'disconnected':
          case 'closed':
          case 'failed':
            pc.removeEventListener('connectionstatechange', onConnectionStateChange)
            return reject(new Error('WebRTC Peer connection failed'))
          default:
            break
        }
      }
      pc.addEventListener('connectionstatechange', onConnectionStateChange)
      peerConnectedAbortController.signal.addEventListener('abort', () =>
        // reject(new Error('Aborted'))
        resolve(void 0)
      )
    })

    peerConnected
      .then(() => {
        this.#cameraStreamLogger.info('WebRTC Peer connection established')
      })
      .catch((err: Error) => {
        this.#cameraStreamLogger.error(`WebRTC peer connection failed: ${err.message}`)
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
      const { unSubscribe } = event.track.onReceiveRtp.subscribe((rtp) => {
        // Capture the video SSRC from the first RTP packet so we can address PLI correctly.
        if (event.track.kind === 'video' && this.#videoSsrc === undefined) {
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
    if (this.#singleProcessMode) {
      this.#singleProcessCameraActive = true
      this.#detectedCameraForCurrentCycle = false
    }
    this.#cameraStreamLogger.info(`Starting FFMpeg with WebRTC stream`)

    const ffmpegArgs: string[] = [
      '-y', // Overwrite output files
      '-hide_banner', // Hide FFmpeg banner
      '-loglevel',
      env.get('FFMPEG_LOG_LEVEL', 'warning'), // Log level set to warning
      '-protocol_whitelist',
      'file,crypto,data,udp,rtp',
      '-fflags',
      '+discardcorrupt+nobuffer', // Ignore corrupted frames and minimize buffering

      // Limit avformat_find_stream_info to 100ms instead of the default 5 seconds.
      // The SDP already specifies the codec (H264 video, OPUS audio), so ffmpeg
      // does not need to wait for actual RTP packets to know the stream layout.
      // Without this, camera ffmpeg blocks for up to 5s before opening camera.sock,
      // which is longer than the ~1.3s window before mediamtx's unDemand event
      // kills the nestmtx:stream process.
      '-analyzeduration',
      '100000', // 100ms in microseconds

      // No hwaccel decoding args here: -c:v copy means the H264 bitstream is
      // forwarded as-is without decoding. Specifying -hwaccel vaapi alongside
      // -c:v copy causes ffmpeg to attempt VAAPI device init and then hang
      // because the device is already held by the VAAPI output streamer.

      // SDP input
      '-i',
      this.#streamerFFMpegInputSdp,

      // Pass through video without re-encoding
      '-c:v',
      'copy',

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
      '0:v', // Map the video input to the H.264 video stream
      '-map',
      '0:a', // Map the original AAC audio to the first audio track
      '-map',
      '0:a', // Map the original audio again for Opus encoding

      // Muxing into MPEG-TS
      '-f',
      'mpegts',
      '-muxdelay',
      '0.2', // Set muxing delay
      '-muxpreload',
      '0.1', // Set mux preload

      // Limit threads before the output URL (trailing options after the URL are ignored)
      '-threads',
      '1',

      // Output to Unix socket
      `unix:${this.#cameraPassthroughSock}`,
    ]

    // Spawn camera ffmpeg BEFORE the output-streamer restart so it opens the
    // UDP ports immediately. SPS/PPS NAL units are sent by the WebRTC peer at
    // the very start of the H264 stream. Waiting until after the restart (~1s)
    // means they're gone and the H264 parser can never find frame boundaries,
    // producing "non-existing PPS referenced" errors for the entire session.
    // Data written to camera.sock during the restart is dropped by the
    // #outputStreamerIsRestarting guard, which is correct.
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
      if (isUnexpected && this.#singleProcessMode) {
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
          `single-process mode: camera crashed unexpectedly (code=${code}) — recovering in-place, failure #${this.#singleProcessReinitCount} (detectedSize=${this.#detectedCameraSize ?? 'none'} detectedProfile=${this.#detectedCameraProfile ?? 'none'})`
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
        if (!this.#singleProcessMode) {
          // Restart-based mode: restart back to software before starting a new
          // placeholder phase (the next #webrtcStart call starts a new static input
          // and will restart to VAAPI again when the camera reconnects).
          void this.#restartOutputStreamer(false)
        } else {
          // Single-process mode: output streamer stays in VAAPI mode throughout.
          // The new static input will be fed as H264 to the same running VAAPI
          // output streamer; the VAAPI decoder sees another SPS transition (live
          // camera → placeholder H264) which is also part of what we're testing.
          this.#singleProcessCameraActive = false
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
        `[diag T+2s] cameraStreamer pid=${diagnosticRef.pid} exitCode=${diagnosticRef.exitCode ?? 'null(running)'} | outputStreamer pid=${this.#streamer?.pid} exitCode=${this.#streamer?.exitCode ?? 'null(running)'}`
      )
    }, 2000)

    if (!this.#singleProcessMode) {
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

      await this.#restartOutputStreamer(this.#isVaapiEnabled)

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
