export class TelegramBridge {
  async inbound(): Promise<void> {
    // TODO: normalize inbound human messages to envelope
  }

  async outbound(): Promise<void> {
    // TODO: send envelope payloads to Telegram
  }
}
