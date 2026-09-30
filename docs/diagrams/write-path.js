import { diagram } from "./lib.js";

diagram(1400, 680, ({ box, arrow, text, note, title, C }) => {
  title("Write path: many Puts, one fsync");

  const callers = ["Put(k1)", "Put(k2)", "Delete(k3)", "Put(k4)"].map((label, i) =>
    box(40, 150 + i * 75, 190, 54, { fill: C.gray, label, font: "Cascadia", size: 16 }),
  );

  const writer = box(300, 120, 340, 350, { fill: C.purple });
  text(writer.cx, 150, "writer goroutine", { size: 20 });
  const steps = [
    ["drain every queued request", C.white],
    ["one write + one fsync", C.yellow],
    ["apply batch to memtable", C.blue],
    ["ack every caller", C.white],
  ].map(([label, fill], i) => box(325, 185 + i * 68, 290, 52, { fill, label, size: 17 }));

  callers.forEach((c, i) => arrow([[c.right + 6, c.cy], [writer.x - 4, 200 + i * 60]]));

  const wal = box(740, 150, 280, 90, { fill: C.yellow, label: "WAL\n000007.wal", size: 19 });
  note(wal.cx, 130, "append-only, CRC32C per record");
  arrow([[steps[1].right + 4, steps[1].cy], [wal.x - 4, wal.cy]]);

  const mem = box(740, 290, 280, 90, { fill: C.blue, label: "memtable\nkeys + tombstones", size: 19 });
  arrow([[steps[2].right + 4, steps[2].cy], [mem.x - 4, mem.cy]]);

  const sst = box(740, 510, 280, 80, { fill: C.green, label: "SSTable 000008.sst", size: 19 });
  arrow([[mem.cx, mem.bottom + 4], [sst.cx, sst.y - 4]]);
  note(mem.cx - 14, 445, "flush at 4 MiB", { anchor: "end" });

  const manifest = box(1110, 510, 250, 80, { fill: C.orange, label: "MANIFEST\ntables 3 8 · log 9", size: 18 });
  arrow([[sst.right + 4, sst.cy], [manifest.x - 4, manifest.cy]]);
  note(manifest.cx, manifest.bottom + 22, "atomic rename = the commit point");

  const compact = box(1110, 280, 250, 110, { fill: C.gray, dashed: true, label: "compaction\n4 tables → 1\ntombstones dropped", size: 16 });
  arrow([[sst.right - 20, sst.y - 4], [compact.x - 4, compact.cy + 20]], { dashed: true });
  arrow([[compact.cx, compact.bottom + 4], [manifest.cx, manifest.y - 4]]);

  arrow([[steps[3].cx, writer.bottom + 4], [420, 600], [200, 610], [callers[3].cx, callers[3].bottom + 6]], { curve: true });
  note(330, 640, "each caller returns only after the fsync that covers its record");
});
