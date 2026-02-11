export class OpenClawBridge {
  async ingestSessionEvent(): Promise<void> {
    // TODO: map OpenClaw session events -> envelope
  }

  async emitSessionEvent(): Promise<void> {
    // TODO: map envelope -> OpenClaw session/tool event
  }
}
