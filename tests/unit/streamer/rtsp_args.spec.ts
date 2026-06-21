import { test } from '@japa/runner'
import { buildRtspCameraFfmpegArgs, getRtspCharacteristicsRetryDelayMs } from '#utilities/streamer_args'

const RTSP_SRC = 'rtsps://stream.googlenest.com/camera123'
const OUTPUT_PATH = 'unix:/tmp/nestmtx/camera.sock'

const base = {
  logLevel: 'warning',
  rtspSrc: RTSP_SRC,
  outputPath: OUTPUT_PATH,
}

test.group('buildRtspCameraFfmpegArgs — input and output', () => {
  test('uses the provided RTSP source URL as input', ({ assert }) => {
    const args = buildRtspCameraFfmpegArgs(base)
    const idx = args.indexOf('-i')
    assert.notEqual(idx, -1)
    assert.equal(args[idx + 1], RTSP_SRC)
  })

  test('uses the provided output path as the last argument', ({ assert }) => {
    const args = buildRtspCameraFfmpegArgs(base)
    assert.equal(args[args.length - 1], OUTPUT_PATH)
  })

  test('uses udp for rtsp_transport', ({ assert }) => {
    const args = buildRtspCameraFfmpegArgs(base)
    const idx = args.indexOf('-rtsp_transport')
    assert.notEqual(idx, -1)
    assert.equal(args[idx + 1], 'udp')
  })

  test('includes -re to rate-limit reads to stream timestamps', ({ assert }) => {
    const args = buildRtspCameraFfmpegArgs(base)
    assert.include(args, '-re')
  })
})

test.group('buildRtspCameraFfmpegArgs — codec settings', () => {
  test('copies video with -c:v copy', ({ assert }) => {
    const args = buildRtspCameraFfmpegArgs(base)
    const idx = args.indexOf('-c:v')
    assert.equal(args[idx + 1], 'copy')
  })

  test('encodes first audio track as AAC', ({ assert }) => {
    const args = buildRtspCameraFfmpegArgs(base)
    const idx = args.indexOf('-c:a:0')
    assert.equal(args[idx + 1], 'aac')
  })

  test('encodes second audio track as libopus', ({ assert }) => {
    const args = buildRtspCameraFfmpegArgs(base)
    const idx = args.indexOf('-c:a:1')
    assert.equal(args[idx + 1], 'libopus')
  })

  test('outputs mpegts format', ({ assert }) => {
    const args = buildRtspCameraFfmpegArgs(base)
    const idx = args.indexOf('-f')
    assert.equal(args[idx + 1], 'mpegts')
  })

  test('limits threads to 1', ({ assert }) => {
    const args = buildRtspCameraFfmpegArgs(base)
    const idx = args.indexOf('-threads')
    assert.equal(args[idx + 1], '1')
  })

  test('uses 100ms analyzeduration to avoid 5s stream probe delay', ({ assert }) => {
    const args = buildRtspCameraFfmpegArgs(base)
    const idx = args.indexOf('-analyzeduration')
    assert.equal(args[idx + 1], '100000')
  })
})

test.group('buildRtspCameraFfmpegArgs — log level', () => {
  test('uses the provided log level', ({ assert }) => {
    const args = buildRtspCameraFfmpegArgs({ ...base, logLevel: 'info' })
    const idx = args.indexOf('-loglevel')
    assert.notEqual(idx, -1)
    assert.equal(args[idx + 1], 'info')
  })

  test('defaults to warning log level', ({ assert }) => {
    const args = buildRtspCameraFfmpegArgs(base)
    const idx = args.indexOf('-loglevel')
    assert.equal(args[idx + 1], 'warning')
  })
})

test.group('getRtspCharacteristicsRetryDelayMs — exponential backoff', () => {
  test('attempt 0 gives 1s delay', ({ assert }) => {
    assert.equal(getRtspCharacteristicsRetryDelayMs(0), 1000)
  })

  test('attempt 1 gives 2s delay', ({ assert }) => {
    assert.equal(getRtspCharacteristicsRetryDelayMs(1), 2000)
  })

  test('attempt 2 gives 4s delay', ({ assert }) => {
    assert.equal(getRtspCharacteristicsRetryDelayMs(2), 4000)
  })

  test('attempt 3 gives 8s delay', ({ assert }) => {
    assert.equal(getRtspCharacteristicsRetryDelayMs(3), 8000)
  })

  test('attempt 6 gives 64s delay (cap)', ({ assert }) => {
    assert.equal(getRtspCharacteristicsRetryDelayMs(6), 64000)
  })

  test('delay never exceeds 64s cap regardless of attempt count', ({ assert }) => {
    assert.equal(getRtspCharacteristicsRetryDelayMs(7), 64000)
    assert.equal(getRtspCharacteristicsRetryDelayMs(50), 64000)
    assert.equal(getRtspCharacteristicsRetryDelayMs(1000), 64000)
  })

  test('each attempt doubles the previous until the cap', ({ assert }) => {
    const delays = [0, 1, 2, 3, 4, 5].map(getRtspCharacteristicsRetryDelayMs)
    for (let i = 1; i < delays.length; i++) {
      assert.equal(delays[i], delays[i - 1] * 2, `attempt ${i} should be double attempt ${i - 1}`)
    }
  })
})
