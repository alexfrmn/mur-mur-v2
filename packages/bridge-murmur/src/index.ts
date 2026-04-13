export interface MurmurCheckpointStore {
  getLastSeen(peerId: string): Promise<string | null>;
  setLastSeen(peerId: string, messageId: string): Promise<void>;
}

export class MurmurBridge {
  async pollAndForward(): Promise<void> {
    // TODO: call murmur CLI/API, map to canonical envelope, publish to bus
  }
}
