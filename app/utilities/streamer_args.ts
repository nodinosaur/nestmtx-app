export interface OutputStreamerArgsOptions {
  logLevel: string
  hwAccel: string
  accelDevice: string
  persistentMode: boolean
  cameraRelayPort: number | undefined
  destination: string
}

export function buildOutputStreamerArgs(opts: OutputStreamerArgsOptions): string[] {
  const { logLevel, hwAccel, accelDevice, persistentMode, cameraRelayPort, destination } = opts

  const isVaapi = hwAccel === 'vaapi'
  const isCuda = hwAccel === 'nvenc' || hwAccel === 'nvdec' || hwAccel === 'cuvid'
  const isVideoToolbox = hwAccel === 'videotoolbox'
  const isQsv = hwAccel === 'qsv'

  // SRT relay provides proper stream framing so VAAPI can establish its hwaccel
  // decode context. pipe:3 MPEG-TS does not — ffmpeg falls back to h264 (native)
  // software decode. Other accelerators (CUDA, QSV, VideoToolbox) work fine with
  // pipe:3 so the relay is VAAPI-specific.
  const useSrtRelay = isVaapi && cameraRelayPort !== undefined
  // Persistent mode: bypass all hwaccel; just copy h264 bytes through pipe:3
  // unchanged. No filter chain → no reinit errors when source switches.
  const useCopyMode = persistentMode

  const args: string[] = [
    '-loglevel',
    logLevel,
    '-fflags',
    '+discardcorrupt+genpts',
    '-avoid_negative_ts',
    'make_zero',
  ]

  if (!useCopyMode) {
    if (isVaapi) {
      // init_hw_device + filter_hw_device: explicit, portable form — -vaapi_device
      // shorthand does not propagate to filtergraphs on some driver versions.
      args.push(
        '-init_hw_device', `vaapi=va:${accelDevice || '/dev/dri/renderD128'}`,
        '-filter_hw_device', 'va',
      )
      if (useSrtRelay) {
        // SRT input: VAAPI hw decode can initialise from the network stream.
        // Frames exit the decoder as vaapi surfaces → scale_vaapi needs no hwupload.
        args.push(
          '-hwaccel', 'vaapi',
          '-hwaccel_output_format', 'vaapi',
          '-hwaccel_device', 'va',
          '-extra_hw_frames', '64',
        )
      }
      // pipe:3 mode: hwaccel decode is intentionally omitted.
      // With -hwaccel vaapi on a pipe:3 input, ffmpeg cannot establish the VAAPI
      // decode context from the MPEG-TS headers alone, so it silently falls back to
      // h264 (native) software decode. Later, when live camera H264 arrives with
      // richer SPS/PPS, VAAPI decode CAN init — causing the decoder to switch from
      // h264 (native) to h264_vaapi mid-stream. That triggers a filter-graph
      // reconfiguration (yuvj420p → vaapi) that scale_vaapi cannot survive.
    } else if (isCuda) {
      args.push(
        '-hwaccel', 'cuda',
        '-hwaccel_output_format', 'cuda',
        '-extra_hw_frames', '64',
      )
      if (accelDevice) args.push('-hwaccel_device', accelDevice)
    } else if (isVideoToolbox) {
      args.push('-hwaccel', 'videotoolbox')
    } else if (isQsv) {
      args.push('-hwaccel', 'qsv', '-hwaccel_output_format', 'qsv')
      if (accelDevice) args.push('-qsv_device', accelDevice)
    }
  }

  // Applied regardless of encoder: on severely corrupted/lossy RTP input the
  // decoder can spin producing rapid "Invalid data found" errors rather than
  // dropping bad frames and continuing. ignore_err absorbs these silently.
  args.push('-err_detect', 'ignore_err')

  if (useSrtRelay) {
    args.push('-i', `srt://127.0.0.1:${cameraRelayPort}`)
  } else {
    args.push('-i', 'pipe:3')
  }

  if (useCopyMode) {
    // Persistent mode: pass h264 bytes through unchanged — no decode, no encode,
    // no filter chain. The output streamer never needs to restart because there
    // is nothing to reinitialise when the source switches from placeholder to camera.
    args.push('-c:v', 'copy')
  } else if (isVaapi) {
    if (useSrtRelay) {
      // Frames are already VAAPI surfaces from hw decode — no hwupload needed.
      args.push('-vf', 'scale_vaapi=format=nv12')
    } else {
      // Software-decoded frames are in CPU memory (yuvj420p). hwupload transfers
      // them to GPU, then scale_vaapi converts to NV12 — the format h264_vaapi
      // requires. The filter chain stays stable because the decoder never changes.
      args.push('-vf', 'hwupload,scale_vaapi=format=nv12')
    }
    args.push('-c:v', 'h264_vaapi', '-b:v', '2M', '-maxrate', '2M')
  } else if (isCuda) {
    // CUDA-decoded frames are already in GPU memory (NV12); h264_nvenc accepts
    // them directly without an intermediate format filter.
    args.push('-c:v', 'h264_nvenc', '-preset', 'p4', '-b:v', '2M', '-maxrate', '2M')
  } else if (isVideoToolbox) {
    // VideoToolbox: -hwaccel videotoolbox decodes on GPU; h264_videotoolbox
    // encodes on GPU. No format filter needed — VT surfaces pass through directly.
    args.push('-c:v', 'h264_videotoolbox', '-b:v', '2M', '-maxrate', '2M')
  } else if (isQsv) {
    // QSV: decoder output surfaces feed h264_qsv directly on the Intel GPU.
    args.push('-c:v', 'h264_qsv', '-b:v', '2M', '-maxrate', '2M')
  } else {
    args.push(
      '-c:v', 'libx264',
      '-preset', 'veryfast',
      '-tune', 'zerolatency',
      '-vf', 'fps=15',
      '-b:v', '2M',
      '-maxrate', '2M',
    )
  }

  args.push(
    // Audio: copy both AAC and Opus tracks produced by upstream ffmpeg (static
    // or camera). Re-encoding here wastes 4 software codec operations per frame
    // without changing the codec, format, or bitrate — upstream already encoded
    // to the required settings.
    '-c:a', 'copy',
    '-map', '0:v:0',
    '-map', '0:a:0',
    '-map', '0:a:1',
    '-f', 'mpegts',
    destination,
  )

  return args
}

export interface CameraFfmpegArgsOptions {
  logLevel: string
  sdpPath: string
  outputPath: string
  persistentMode: boolean
}

export function buildCameraFfmpegArgs(opts: CameraFfmpegArgsOptions): string[] {
  const { logLevel, sdpPath, outputPath, persistentMode } = opts

  const args: string[] = [
    '-y',
    '-hide_banner',
    '-loglevel',
    logLevel,
    '-protocol_whitelist',
    'file,crypto,data,udp,rtp',
    '-fflags',
    '+discardcorrupt+nobuffer',
    '-analyzeduration',
    '100000',
    '-err_detect',
    'ignore_err',
    '-i',
    sdpPath,
    '-c:v',
    'copy',
    '-c:a:0',
    'aac',
    '-b:a:0',
    '128k',
    '-c:a:1',
    'libopus',
    '-b:a:1',
    '128k',
    '-map',
    '0:v',
    '-map',
    '0:a',
    '-map',
    '0:a',
    '-f',
    'mpegts',
    '-muxdelay',
    '0.2',
    '-muxpreload',
    '0.1',
    '-threads',
    '1',
    outputPath,
  ]

  // Persistent mode: stamp camera MPEG-TS with wall-clock time so its timestamps
  // are continuous with the static placeholder (which also uses wall-clock time).
  // Without this, camera timestamps come from the RTP epoch (a much smaller number),
  // the DTS jumps backwards when the source switches, and VLC stalls for ~40s as
  // it buffers to reach the apparent "position" in the stream.
  if (persistentMode) {
    args.splice(args.indexOf('-i'), 0, '-use_wallclock_as_timestamps', '1')
  }

  return args
}
