ALTER TABLE ad_click_counts_minute
  ADD COLUMN last_offset BIGINT NOT NULL DEFAULT -1;
