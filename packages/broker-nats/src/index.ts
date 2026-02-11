export interface BrokerConfig {
  url: string;
  stream: string;
}

export class NatsBroker {
  constructor(private readonly config: BrokerConfig) {}

  async publish(subject: string, payload: string): Promise<void> {
    // TODO: JetStream producer implementation
    void subject; void payload; void this.config;
  }

  async subscribe(subject: string): Promise<void> {
    // TODO: durable consumer + ack/nack + retry + DLQ
    void subject;
  }
}
