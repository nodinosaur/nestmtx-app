import { test } from '@japa/runner'
import { buildCameraFfmpegArgs } from '#utilities/streamer_args'

const base = {
  logLevel: 'warning',
  sdpPath: '/tmp/nestmtx/test.sdp',
  outputPath: 'unix:/tmp/nestmtx/camera.sock',
  persistentMode: false,
}

test.group('buildCameraFfmpegArgs — persistent mode timestamp fix', () => {
  test('inserts -use_wallclock_as_timestamps 1 before -i when persistent', ({ assert }) => {
    const args = buildCameraFfmpegArgs({ ...base, persistentMode: true })
    const tsIdx = args.indexOf('-use_wallclock_as_timestamps')
    const iIdx = args.indexOf('-i')
    assert.notEqual(tsIdx, -1, '-use_wallclock_as_timestamps must be present')
    assert.equal(args[tsIdx + 1], '1')
    assert.isBelow(tsIdx, iIdx, '-use_wallclock_as_timestamps must appear before -i')
  })

  test('does NOT include -use_wallclock_as_timestamps when not persistent', ({ assert }) => {
    const args = buildCameraFfmpegArgs({ ...base, persistentMode: false })
    assert.notInclude(args, '-use_wallclock_as_timestamps')
  })
})

test.group('buildCameraFfmpegArgs — log level', () => {
  test('uses FFMPEG_DEBUG_LEVEL value (passed as logLevel)', ({ assert }) => {
    const args = buildCameraFfmpegArgs({ ...base, logLevel: 'info' })
    const idx = args.indexOf('-loglevel')
    assert.notEqual(idx, -1)
    assert.equal(args[idx + 1], 'info')
  })

  test('defaults to warning log level', ({ assert }) => {
    const args = buildCameraFfmpegArgs(base)
    const idx = args.indexOf('-loglevel')
    assert.equal(args[idx + 1], 'warning')
  })
})

test.group('buildCameraFfmpegArgs — input and output routing', () => {
  test('uses the provided SDP path as input', ({ assert }) => {
    const args = buildCameraFfmpegArgs(base)
    const idx = args.indexOf('-i')
    assert.equal(args[idx + 1], base.sdpPath)
  })

  test('uses unix socket path when no SRT relay (non-persistent)', ({ assert }) => {
    const args = buildCameraFfmpegArgs(base)
    assert.include(args, 'unix:/tmp/nestmtx/camera.sock')
  })

  test('uses SRT relay URL when provided as outputPath', ({ assert }) => {
    const srtPath = 'srt://127.0.0.1:25001?mode=listener&pkt_size=1316'
    const args = buildCameraFfmpegArgs({ ...base, outputPath: srtPath })
    assert.include(args, srtPath)
  })

  test('SDP path appears as -i value even in persistent mode', ({ assert }) => {
    const args = buildCameraFfmpegArgs({ ...base, persistentMode: true })
    const idx = args.indexOf('-i')
    assert.equal(args[idx + 1], base.sdpPath)
  })
})

test.group('buildCameraFfmpegArgs — codec and mux settings', () => {
  test('passes through video with -c:v copy', ({ assert }) => {
    const args = buildCameraFfmpegArgs(base)
    const idx = args.indexOf('-c:v')
    assert.equal(args[idx + 1], 'copy')
  })

  test('encodes first audio track as AAC', ({ assert }) => {
    const args = buildCameraFfmpegArgs(base)
    const idx = args.indexOf('-c:a:0')
    assert.equal(args[idx + 1], 'aac')
  })

  test('encodes second audio track as libopus', ({ assert }) => {
    const args = buildCameraFfmpegArgs(base)
    const idx = args.indexOf('-c:a:1')
    assert.equal(args[idx + 1], 'libopus')
  })

  test('outputs mpegts format', ({ assert }) => {
    const args = buildCameraFfmpegArgs(base)
    const idx = args.indexOf('-f')
    assert.equal(args[idx + 1], 'mpegts')
  })

  test('limits threads to 1', ({ assert }) => {
    const args = buildCameraFfmpegArgs(base)
    const idx = args.indexOf('-threads')
    assert.equal(args[idx + 1], '1')
  })

  test('uses 100ms analyzeduration to avoid 5s stream probe delay', ({ assert }) => {
    const args = buildCameraFfmpegArgs(base)
    const idx = args.indexOf('-analyzeduration')
    assert.equal(args[idx + 1], '100000')
  })
})
