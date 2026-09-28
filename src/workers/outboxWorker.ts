import { pool } from '../infrastructure/db/pool.js';
import { config } from '../infrastructure/config.js';
import { OutboxMessage } from '../domain/types.js';

export class OutboxWorker {
  private isRunning = false;
  private timer: NodeJS.Timeout | null = null;

  async start(): Promise<void> {
    this.isRunning = true;
    console.log(`[Outbox Worker] Started with interval: ${config.outboxPollIntervalMs}ms`);
    this.scheduleNext();
  }

  stop(): void {
    this.isRunning = false;
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
    console.log('[Outbox Worker] Stopped.');
  }

  private scheduleNext(): void {
    if (!this.isRunning) return;
    this.timer = setTimeout(async () => {
      try {
        await this.processBatch();
      } catch (err) {
        console.error('[Outbox Worker] Error in polling loop:', err);
      } finally {
        this.scheduleNext();
      }
    }, config.outboxPollIntervalMs);
  }

  async processBatch(): Promise<number> {
    const client = await pool.connect();
    try {
      await client.query('BEGIN');

      // Poll pending outbox records with FOR UPDATE SKIP LOCKED
      const res = await client.query<OutboxMessage>(
        `SELECT id, aggregate_type, aggregate_id, event_type, payload, retry_count
         FROM outbox_messages
         WHERE status = 'PENDING'
         ORDER BY created_at ASC
         LIMIT $1
         FOR UPDATE SKIP LOCKED`,
        [config.outboxBatchSize]
      );

      if (res.rows.length === 0) {
        await client.query('COMMIT');
        return 0;
      }

      console.log(`[Outbox Worker] Processing batch of ${res.rows.length} messages...`);

      const messageIds: string[] = [];
      for (const msg of res.rows) {
        try {
          // Simulate message broker dispatch (e.g. Kafka producer or RabbitMQ channel)
          await this.dispatchToBroker(msg);
          messageIds.push(msg.id);
        } catch (dispatchErr) {
          console.error(`[Outbox Worker] Failed to dispatch message ${msg.id}:`, dispatchErr);
          await client.query(
            `UPDATE outbox_messages 
             SET retry_count = retry_count + 1, 
                 status = CASE WHEN retry_count >= 5 THEN 'FAILED' ELSE 'PENDING' END
             WHERE id = $1`,
            [msg.id]
          );
        }
      }

      if (messageIds.length > 0) {
        await client.query(
          `UPDATE outbox_messages
           SET status = 'PUBLISHED', published_at = NOW()
           WHERE id = ANY($1::uuid[])`,
          [messageIds]
        );
      }

      await client.query('COMMIT');
      console.log(`[Outbox Worker] Successfully dispatched and marked ${messageIds.length} messages.`);
      return messageIds.length;
    } catch (err) {
      await client.query('ROLLBACK');
      throw err;
    } finally {
      client.release();
    }
  }

  private async dispatchToBroker(message: OutboxMessage): Promise<void> {
    // In production, this emits to Apache Kafka / RabbitMQ:
    // await kafkaProducer.send({ topic: 'core.ledger.transfers.v1', messages: [{ value: JSON.stringify(message.payload) }] });
    // In local engine, logs structured event envelope:
    console.log(`[Broker Event Emitted] [${message.event_type}] Aggregate: ${message.aggregate_id}`);
  }
}

if (process.argv[1] && process.argv[1].endsWith('outboxWorker.ts')) {
  const worker = new OutboxWorker();
  worker.start().catch((err) => {
    console.error('[Outbox Worker] Fatal crash:', err);
    process.exit(1);
  });
}
