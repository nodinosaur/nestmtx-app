import { CronJob } from '#services/cron'
import type { ApplicationService } from '@adonisjs/core/types'

export default class ZeroReaderTeardownJob extends CronJob {
  #app: ApplicationService
  constructor(protected app: ApplicationService) {
    super(app)
    this.#app = app
  }
  get crontab() {
    return '*/5 * * * * *'
  }

  async run() {
    if (!this.#app) {
      return
    }
    return await this.#app.streamer.zeroReaderCronjob()
  }
}
