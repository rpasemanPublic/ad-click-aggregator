ALTER TABLE ad_click_counts_minute
  DROP COLUMN last_offset,
  ADD COLUMN shard_offsets JSONB NOT NULL DEFAULT '{}'::jsonb;
