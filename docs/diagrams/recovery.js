import { diagram } from "./lib.js";

diagram(1400, 720, ({ box, arrow, text, note, title, C }) => {
  title("Recovery: the MANIFEST is the only source of truth");

  const manifest = box(40, 150, 250, 120, { fill: C.orange, label: "MANIFEST\ntables 3 8\nlog 9", size: 20 });
  note(manifest.cx, manifest.bottom + 28, "replaced atomically:\ntmp → fsync → rename → fsync dir");

  const dir = box(360, 120, 450, 500, { fill: C.gray });
  text(dir.cx, 150, "data directory after a crash", { size: 19 });
  arrow([[manifest.right + 6, manifest.cy], [dir.x - 6, manifest.cy]], { dashed: true });

  const rows = [
    ["000003.sst", C.green, "keep: listed in MANIFEST"],
    ["000008.sst", C.green, "keep: listed in MANIFEST"],
    ["000010.sst", C.red, "delete: not listed, left by an\ninterrupted flush or compaction"],
    ["000007.wal", C.red, "delete: below log 9, already flushed;\nreplaying would resurrect old values"],
    ["000009.wal", C.blue, "replay into the memtable"],
    ["000011.sst.tmp", C.red, "delete: interrupted atomic write"],
  ];
  rows.forEach(([file, fill, verdict], i) => {
    const y = 185 + i * 70;
    const f = box(390, y, 390, 52, { fill: C.white, label: file, font: "Cascadia", size: 17 });
    const v = box(900, y - 4, 460, 60, { fill, label: verdict, size: 15 });
    arrow([[f.right + 6, f.cy], [v.x - 6, v.cy]]);
  });

  note(dir.cx, 660, "SSTables on disk but no MANIFEST → refuse to open rather than guess");
});
