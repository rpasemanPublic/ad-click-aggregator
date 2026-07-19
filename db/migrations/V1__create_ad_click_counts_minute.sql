CREATE TABLE ad_click_counts_minute (
  ad_id        TEXT        NOT NULL,
  window_start TIMESTAMPTZ NOT NULL,
  click_count  BIGINT      NOT NULL,
  PRIMARY KEY (ad_id, window_start)
);
