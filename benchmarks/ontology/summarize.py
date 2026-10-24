"""Print medians across repetitions; never pool or average reported percentiles."""

import json
import statistics
import sys

report = json.load(open(sys.argv[1], encoding="utf-8"))
print("Completed:", report.get("completed", False))
print(
    "rows clients implementation rps queue_p95_range detail_p95_range action_p95_range peak_rss_MiB app_cpu generator_cpu failures"
)
for volume in report["volumes"]:
    for clients in report["settings"]["clients"]:
        for name in report["settings"].get("implementations", ["node", "go", "python"]):
            trials = [
                t
                for t in volume["trials"]
                if t["clients"] == clients and t["implementation"] == name
            ]
            if not trials:
                continue

            def median(field):
                return statistics.median(t[field] for t in trials)

            ranges = []
            for op in ("queue", "detail", "action"):
                values = [t["operations"][op]["p95Ms"] for t in trials]
                ranges.append(f"{min(values):.1f}–{max(values):.1f}")
            errors = sum(
                o["attempts"] - o["success"] for t in trials for o in t["operations"].values()
            )
            print(
                volume["rows"],
                clients,
                name,
                round(median("successfulRps")),
                *ranges,
                round(max(t["appPeakRssKiB"] for t in trials) / 1024, 1),
                round(median("appCpuCores"), 2),
                round(median("generatorCpuCores"), 2),
                errors,
            )
    print(
        "Storage:", volume["storage"], "incremental seed seconds", round(volume["seedMs"] / 1000, 1)
    )
    for name, plans in volume["sql"].items():
        print(name, "execution ms:", [p["plan"]["Execution Time"] for p in plans])
