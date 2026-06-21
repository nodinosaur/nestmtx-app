import { test } from '@japa/runner'
import {
  getHardwareAcceleratedDecodingArgumentsFor,
  getHardwareAcceleratedEncodingArgumentsFor,
} from '#utilities/ffmpeg'

test.group('getHardwareAcceleratedDecodingArgumentsFor', () => {
  test('returns empty array for unknown/software accelerator', ({ assert }) => {
    assert.deepEqual(getHardwareAcceleratedDecodingArgumentsFor(''), [])
    assert.deepEqual(getHardwareAcceleratedDecodingArgumentsFor('unknown'), [])
  })

  test('vaapi uses init_hw_device and filter_hw_device (not -vaapi_device shorthand)', ({ assert }) => {
    const args = getHardwareAcceleratedDecodingArgumentsFor('vaapi', '/dev/dri/renderD128')
    assert.include(args, '-init_hw_device')
    assert.include(args, '-filter_hw_device')
    assert.notInclude(args, '-vaapi_device')
  })

  test('vaapi sets hwaccel_output_format to vaapi', ({ assert }) => {
    const args = getHardwareAcceleratedDecodingArgumentsFor('vaapi', '/dev/dri/renderD128')
    const idx = args.indexOf('-hwaccel_output_format')
    assert.notEqual(idx, -1)
    assert.equal(args[idx + 1], 'vaapi')
  })

  test('vaapi uses the provided device path in init_hw_device', ({ assert }) => {
    const args = getHardwareAcceleratedDecodingArgumentsFor('vaapi', '/dev/dri/renderD129')
    const deviceArg = args[args.indexOf('-init_hw_device') + 1]
    assert.include(deviceArg, '/dev/dri/renderD129')
  })

  test('vaapi falls back to renderD128 when no device provided', ({ assert }) => {
    const args = getHardwareAcceleratedDecodingArgumentsFor('vaapi')
    const deviceArg = args[args.indexOf('-init_hw_device') + 1]
    assert.include(deviceArg, '/dev/dri/renderD128')
  })

  test('cuda/nvenc/nvdec/cuvid all produce cuda hwaccel args', ({ assert }) => {
    for (const accel of ['nvenc', 'nvdec', 'cuvid']) {
      const args = getHardwareAcceleratedDecodingArgumentsFor(accel)
      assert.include(args, '-hwaccel', `${accel} should include -hwaccel`)
      const idx = args.indexOf('-hwaccel')
      assert.equal(args[idx + 1], 'cuda')
    }
  })

  test('cuda includes extra_hw_frames', ({ assert }) => {
    const args = getHardwareAcceleratedDecodingArgumentsFor('nvenc')
    assert.include(args, '-extra_hw_frames')
  })

  test('qsv sets hwaccel_output_format to qsv', ({ assert }) => {
    const args = getHardwareAcceleratedDecodingArgumentsFor('qsv')
    const idx = args.indexOf('-hwaccel_output_format')
    assert.notEqual(idx, -1)
    assert.equal(args[idx + 1], 'qsv')
  })

  test('videotoolbox uses -hwaccel videotoolbox', ({ assert }) => {
    const args = getHardwareAcceleratedDecodingArgumentsFor('videotoolbox')
    const idx = args.indexOf('-hwaccel')
    assert.notEqual(idx, -1)
    assert.equal(args[idx + 1], 'videotoolbox')
  })
})

test.group('getHardwareAcceleratedEncodingArgumentsFor', () => {
  test('returns libx264 for unknown/empty accelerator', ({ assert }) => {
    const args = getHardwareAcceleratedEncodingArgumentsFor('')
    assert.include(args, 'libx264')
  })

  test('vaapi uses hwupload before scale_vaapi (CPU→GPU upload order)', ({ assert }) => {
    const args = getHardwareAcceleratedEncodingArgumentsFor('vaapi')
    const vfIdx = args.indexOf('-vf')
    assert.notEqual(vfIdx, -1)
    const filter = args[vfIdx + 1]
    assert.match(filter, /hwupload,scale_vaapi/)
  })

  test('vaapi uses h264_vaapi encoder', ({ assert }) => {
    const args = getHardwareAcceleratedEncodingArgumentsFor('vaapi')
    const idx = args.indexOf('-c:v')
    assert.notEqual(idx, -1)
    assert.equal(args[idx + 1], 'h264_vaapi')
  })

  test('cuda/nvenc/nvdec/cuvid all use h264_nvenc encoder', ({ assert }) => {
    for (const accel of ['nvenc', 'nvdec', 'cuvid']) {
      const args = getHardwareAcceleratedEncodingArgumentsFor(accel)
      const idx = args.indexOf('-c:v')
      assert.notEqual(idx, -1, `${accel} should include -c:v`)
      assert.equal(args[idx + 1], 'h264_nvenc')
    }
  })

  test('qsv uses h264_qsv encoder', ({ assert }) => {
    const args = getHardwareAcceleratedEncodingArgumentsFor('qsv')
    const idx = args.indexOf('-c:v')
    assert.equal(args[idx + 1], 'h264_qsv')
  })

  test('videotoolbox uses h264_videotoolbox encoder', ({ assert }) => {
    const args = getHardwareAcceleratedEncodingArgumentsFor('videotoolbox')
    const idx = args.indexOf('-c:v')
    assert.equal(args[idx + 1], 'h264_videotoolbox')
  })

  test('vdpau falls back to libx264 (vdpau is decode-only)', ({ assert }) => {
    const args = getHardwareAcceleratedEncodingArgumentsFor('vdpau')
    const idx = args.indexOf('-c:v')
    assert.equal(args[idx + 1], 'libx264')
  })
})
