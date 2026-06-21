export async function startJetStreamAdvisoryDlqIfEnabled({
  broker,
  outbox,
  jetstreamEnabled,
  log = () => {},
}) {
  if (!jetstreamEnabled) return undefined;
  const subscription = await broker.startJetStreamAdvisoryDlq({ outbox });
  log("info", "JetStream advisory DLQ correlation started");
  return subscription;
}
