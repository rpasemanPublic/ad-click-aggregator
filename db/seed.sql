-- Local dev sample data only. Not a Flyway migration — never intended to run against a
-- real deployment, just to give record-click-service and ad-click-simulator some real
-- ad_ids to work against locally.

INSERT INTO ads (ad_id, destination_url) VALUES
  ('ad-001', 'https://www.anthropic.com'),
  ('ad-002', 'https://www.wikipedia.org'),
  ('ad-003', 'https://www.nasa.gov'),
  ('ad-004', 'https://www.nationalgeographic.com'),
  ('ad-005', 'https://www.mozilla.org')
ON CONFLICT (ad_id) DO NOTHING;
