import { test } from '@japa/runner'
import { buildOutputStreamerArgs } from '#utilities/streamer_args'

const DEST = 'srt://127.0.0.1:8890/?streamid=publish:test&pkt_size=1316'

const base = {
  logLevel: 'warning',
  hwAccel: '',
  accelDevice: '',
  persistentMode: false,
  cameraRelayPort: undefined as number | undefined,
  destination: DEST,
}

test.group('buildOutputStreamerArgs — persistent mode', () => {
  test('uses -c:v copy', ({ assert }) => {
    const args = buildOutputStreamerArgs({ ...base, persistentMode: true, hwAccel: 'vaapi' })
    const idx = args.indexOf('-c:v')
    assert.notEqual(idx, -1)
    assert.equal(args[idx + 1], 'copy')
  })

  test('does not include -init_hw_device even when vaapi is configured', ({ assert }) => {
    const args = buildOutputStreamerArgs({ ...base, persistentMode: true, hwAccel: 'vaapi' })
    assert.notInclude(args, '-init_hw_device')
  })

  test('does not include -filter_hw_device', ({ assert }) => {
    const args = buildOutputStreamerArgs({ ...base, persistentMode: true, hwAccel: 'vaapi' })
    assert.notInclude(args, '-filter_hw_device')
  })

  test('does not include hwupload in any filter', ({ assert }) => {
    const args = buildOutputStreamerArgs({ ...base, persistentMode: true, hwAccel: 'vaapi' })
    assert.notInclude(args.join(' '), 'hwupload')
  })

  test('does not include -hwaccel', ({ assert }) => {
    const args = buildOutputStreamerArgs({ ...base, persistentMode: true, hwAccel: 'vaapi' })
    assert.notInclude(args, '-hwaccel')
  })

  test('reads from pipe:3', ({ assert }) => {
    const args = buildOutputStreamerArgs({ ...base, persistentMode: true })
    const idx = args.indexOf('-i')
    assert.equal(args[idx + 1], 'pipe:3')
  })

  test('still emits destination and mpegts format', ({ assert }) => {
    const args = buildOutputStreamerArgs({ ...base, persistentMode: true })
    assert.include(args, DEST)
    assert.include(args, 'mpegts')
  })
})

test.group('buildOutputStreamerArgs — VAAPI with SRT relay', () => {
  const vaapiSrt = { ...base, hwAccel: 'vaapi', accelDevice: '/dev/dri/renderD128', cameraRelayPort: 25000 }

  test('uses init_hw_device (not -vaapi_device shorthand)', ({ assert }) => {
    const args = buildOutputStreamerArgs(vaapiSrt)
    assert.include(args, '-init_hw_device')
    assert.notInclude(args, '-vaapi_device')
  })

  test('uses the provided device in init_hw_device', ({ assert }) => {
    const args = buildOutputStreamerArgs(vaapiSrt)
    const deviceArg = args[args.indexOf('-init_hw_device') + 1]
    assert.include(deviceArg, '/dev/dri/renderD128')
  })

  test('includes hwaccel decode args for SRT input', ({ assert }) => {
    const args = buildOutputStreamerArgs(vaapiSrt)
    assert.include(args, '-hwaccel')
    assert.include(args, '-hwaccel_output_format')
    assert.include(args, '-extra_hw_frames')
  })

  test('reads from SRT relay URL (not pipe:3)', ({ assert }) => {
    const args = buildOutputStreamerArgs(vaapiSrt)
    const idx = args.indexOf('-i')
    assert.match(args[idx + 1], /^srt:\/\/127\.0\.0\.1:25000/)
  })

  test('uses scale_vaapi WITHOUT hwupload (frames already on GPU)', ({ assert }) => {
    const args = buildOutputStreamerArgs(vaapiSrt)
    const vfIdx = args.indexOf('-vf')
    assert.notEqual(vfIdx, -1)
    const filter = args[vfIdx + 1]
    assert.match(filter, /scale_vaapi/)
    assert.notMatch(filter, /hwupload/)
  })

  test('uses h264_vaapi encoder', ({ assert }) => {
    const args = buildOutputStreamerArgs(vaapiSrt)
    const idx = args.indexOf('-c:v')
    assert.equal(args[idx + 1], 'h264_vaapi')
  })
})

test.group('buildOutputStreamerArgs — VAAPI with pipe:3 (no SRT relay)', () => {
  const vaapiPipe = { ...base, hwAccel: 'vaapi', accelDevice: '/dev/dri/renderD128' }

  test('uses init_hw_device and filter_hw_device', ({ assert }) => {
    const args = buildOutputStreamerArgs(vaapiPipe)
    assert.include(args, '-init_hw_device')
    assert.include(args, '-filter_hw_device')
  })

  test('does NOT include -hwaccel decode args (pipe:3 cannot init VAAPI decode ctx)', ({ assert }) => {
    const args = buildOutputStreamerArgs(vaapiPipe)
    assert.notInclude(args, '-hwaccel_output_format')
    assert.notInclude(args, '-hwaccel_device')
  })

  test('reads from pipe:3', ({ assert }) => {
    const args = buildOutputStreamerArgs(vaapiPipe)
    const idx = args.indexOf('-i')
    assert.equal(args[idx + 1], 'pipe:3')
  })

  test('uses hwupload,scale_vaapi (CPU→GPU upload for software-decoded frames)', ({ assert }) => {
    const args = buildOutputStreamerArgs(vaapiPipe)
    const vfIdx = args.indexOf('-vf')
    assert.notEqual(vfIdx, -1)
    assert.match(args[vfIdx + 1], /^hwupload,scale_vaapi/)
  })

  test('uses h264_vaapi encoder', ({ assert }) => {
    const args = buildOutputStreamerArgs(vaapiPipe)
    const idx = args.indexOf('-c:v')
    assert.equal(args[idx + 1], 'h264_vaapi')
  })
})

test.group('buildOutputStreamerArgs — software fallback', () => {
  test('uses libx264 with no hwAccel', ({ assert }) => {
    const args = buildOutputStreamerArgs(base)
    const idx = args.indexOf('-c:v')
    assert.equal(args[idx + 1], 'libx264')
  })

  test('includes zerolatency tune for libx264', ({ assert }) => {
    const args = buildOutputStreamerArgs(base)
    assert.include(args, 'zerolatency')
  })

  test('no hwaccel args present', ({ assert }) => {
    const args = buildOutputStreamerArgs(base)
    assert.notInclude(args, '-init_hw_device')
    assert.notInclude(args, '-hwaccel')
  })
})

test.group('buildOutputStreamerArgs — audio and common args', () => {
  test('copies audio (-c:a copy)', ({ assert }) => {
    for (const hwAccel of ['', 'vaapi', 'nvenc']) {
      const args = buildOutputStreamerArgs({ ...base, hwAccel })
      const idx = args.indexOf('-c:a')
      assert.equal(args[idx + 1], 'copy', `${hwAccel || 'software'} should copy audio`)
    }
  })

  test('maps video and two audio tracks', ({ assert }) => {
    const args = buildOutputStreamerArgs(base)
    assert.include(args, '0:v:0')
    assert.include(args, '0:a:0')
    assert.include(args, '0:a:1')
  })

  test('outputs to mpegts format', ({ assert }) => {
    const args = buildOutputStreamerArgs(base)
    const idx = args.indexOf('-f')
    assert.equal(args[idx + 1], 'mpegts')
  })

  test('respects the provided log level', ({ assert }) => {
    const args = buildOutputStreamerArgs({ ...base, logLevel: 'info' })
    const idx = args.indexOf('-loglevel')
    assert.equal(args[idx + 1], 'info')
  })
})
