export const getHardwareAcceleratedDecodingArgumentsFor = (
  hwaccel: string,
  hwaccel_device?: string
): Array<string> => {
  switch (hwaccel.toLowerCase()) {
    case 'nvenc':
    case 'nvdec':
    case 'cuvid':
      return [
        '-hwaccel',
        'cuda',
        '-hwaccel_output_format',
        'cuda',
        '-extra_hw_frames',
        '64',
        ...(hwaccel_device ? ['-hwaccel_device', hwaccel_device] : []),
      ]

    case 'vaapi':
      // init_hw_device + filter_hw_device is the explicit, portable form.
      // -vaapi_device is a shorthand that does not propagate to filtergraphs.
      return [
        '-init_hw_device',
        `vaapi=va:${hwaccel_device || '/dev/dri/renderD128'}`,
        '-filter_hw_device',
        'va',
        '-hwaccel',
        'vaapi',
        '-hwaccel_output_format',
        'vaapi',
        '-hwaccel_device',
        'va',
        '-extra_hw_frames',
        '64',
      ]

    case 'qsv':
      return [
        '-hwaccel',
        'qsv',
        '-hwaccel_output_format',
        'qsv',
        ...(hwaccel_device ? ['-qsv_device', hwaccel_device] : []),
      ]

    case 'amf':
      return ['-hwaccel', 'amf']

    case 'vdpau':
      return ['-hwaccel', 'vdpau']

    case 'videotoolbox':
      return ['-hwaccel', 'videotoolbox']

    default:
      return []
  }
}

export const getHardwareAcceleratedEncodingArgumentsFor = (
  hwaccel: string,
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  _hwaccel_device?: string
): Array<string> => {
  switch (hwaccel.toLowerCase()) {
    case 'nvenc':
    case 'nvdec':
    case 'cuvid':
      // Assumes -hwaccel cuda -hwaccel_output_format cuda already in decode args.
      // Frames are already in CUDA memory; h264_nvenc accepts them directly.
      return [
        '-c:v',
        'h264_nvenc',
        '-preset',
        'p4',
      ]

    case 'vaapi':
      // Assumes init_hw_device + filter_hw_device already in decode args.
      // hwupload uploads software-decoded frames to GPU; scale_vaapi converts
      // to NV12 on the GPU (h264_vaapi requires NV12). Do NOT use
      // format=nv12,hwupload — that converts on CPU then uploads (wrong order).
      return [
        '-vf',
        'hwupload,scale_vaapi=format=nv12',
        '-c:v',
        'h264_vaapi',
      ]

    case 'qsv':
      // Assumes -hwaccel qsv -hwaccel_output_format qsv already in decode args.
      // QSV surfaces from the decoder feed h264_qsv directly.
      return [
        '-c:v',
        'h264_qsv',
      ]

    case 'amf':
      return ['-c:v', 'h264_amf']

    case 'vdpau':
      // VDPAU is decode-only; no H.264 hardware encoder available via VDPAU.
      return ['-c:v', 'libx264']

    case 'videotoolbox':
      return ['-c:v', 'h264_videotoolbox']

    default:
      return ['-c:v', 'libx264']
  }
}
