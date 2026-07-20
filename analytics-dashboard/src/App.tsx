import { useEffect, useState } from "react";
import {
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

const API_URL =
  import.meta.env.VITE_ANALYTICS_API_URL ?? "http://localhost:3002";

interface AnalyticsSeries {
  adId: string;
  data: { bucket: string; clicks: number }[];
}

function toChartData(series: AnalyticsSeries[]) {
  const byBucket = new Map<string, Record<string, number | string>>();
  for (const s of series) {
    for (const point of s.data) {
      const row = byBucket.get(point.bucket) ?? { bucket: point.bucket };
      row[s.adId] = point.clicks;
      byBucket.set(point.bucket, row);
    }
  }
  return Array.from(byBucket.values()).sort((a, b) =>
    (a.bucket as string).localeCompare(b.bucket as string),
  );
}

function App() {
  const [ads, setAds] = useState<{ adId: string; destinationUrl: string }[]>(
    [],
  );
  const [selectedAdIds, setSelectedAdIds] = useState<Set<string>>(new Set());
  const [chartData, setChartData] = useState<Record<string, number | string>[]>(
    [],
  );

  async function refresh() {
    if (selectedAdIds.size === 0) {
      setChartData([]);
      return;
    }

    const endTime = new Date();
    const startTime = new Date(endTime.getTime() - 60 * 60 * 1000);

    const params = new URLSearchParams({
      adIds: Array.from(selectedAdIds).join(","),
      startTime: startTime.toISOString(),
      endTime: endTime.toISOString(),
      granularity: "minute",
    });

    const res = await fetch(`${API_URL}/analytics?${params}`);
    const body: { series: AnalyticsSeries[] } = await res.json();
    setChartData(toChartData(body.series));
  }

  function toggleAd(adId: string) {
    setSelectedAdIds((prev) => {
      const next = new Set(prev);
      if (next.has(adId)) {
        next.delete(adId);
      } else {
        next.add(adId);
      }
      return next;
    });
  }

  useEffect(() => {
    fetch(`${API_URL}/ads`)
      .then((res) => res.json())
      .then((body) => setAds(body.ads));
  }, []);

  return (
    <fieldset>
      <legend>Ads</legend>
      {ads.map((ad) => (
        <label key={ad.adId} style={{ display: "block" }}>
          <input
            type="checkbox"
            checked={selectedAdIds.has(ad.adId)}
            onChange={() => toggleAd(ad.adId)}
          />
          {ad.adId} ({ad.destinationUrl})
        </label>
      ))}
      <button type="button" onClick={refresh}>
        Refresh
      </button>
      <LineChart width={800} height={400} data={chartData}>
        <CartesianGrid strokeDasharray="3 3" />
        <XAxis dataKey="bucket" />
        <YAxis />
        <Tooltip />
        <Legend />
        {Array.from(selectedAdIds).map((adId) => (
          <Line key={adId} type="monotone" dataKey={adId} />
        ))}
      </LineChart>
    </fieldset>
  );
}

export default App;
