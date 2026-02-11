export interface MessagingMetrics {
  published: number;
  acked: number;
  nacked: number;
  retried: number;
  dlq: number;
}

export const newMetrics = (): MessagingMetrics => ({
  published: 0,
  acked: 0,
  nacked: 0,
  retried: 0,
  dlq: 0
});
