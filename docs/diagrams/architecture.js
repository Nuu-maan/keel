import { diagram } from "./lib.js";

diagram(1400, 760, ({ box, arrow, text, note, title, C }) => {
  title("Keel: a Raft group of LSM storage nodes");

  const clients = [250, 420].map((y) => box(40, y, 170, 60, { fill: C.gray, label: "client" }));

  const leader = box(290, 130, 440, 500, { fill: C.purple });
  text(leader.cx, 162, "node 1 · leader", { size: 21 });
  const wire = box(320, 195, 380, 72, { label: "wire protocol\nlength-prefixed frames over TCP", size: 16 });
  box(320, 290, 380, 72, { dashed: true, label: "Raft\nelection · log replication · snapshots", size: 16 });
  const engine = box(320, 385, 380, 220, { fill: C.green });
  text(engine.cx, 412, "storage engine", { size: 19 });
  box(340, 440, 165, 60, { fill: C.yellow, label: "WAL\ngroup commit", size: 15 });
  box(515, 440, 165, 60, { fill: C.blue, label: "memtable", size: 16 });
  box(340, 520, 165, 60, { fill: C.white, label: "SSTables\n+ bloom filters", size: 15 });
  box(515, 520, 165, 60, { fill: C.orange, label: "MANIFEST", size: 16 });

  clients.forEach((c) => arrow([[c.right + 6, c.cy], [wire.x - 6, wire.cy + (c.cy < 300 ? -10 : 10)]]));

  const followers = [130, 400].map((y, i) => {
    const f = box(900, y, 300, 230, { fill: C.purple });
    text(f.cx, y + 30, `node ${i + 2} · follower`, { size: 19 });
    box(925, y + 60, 250, 40, { label: "wire protocol", size: 15 });
    box(925, y + 110, 250, 40, { dashed: true, label: "Raft", size: 15 });
    box(925, y + 160, 250, 45, { fill: C.green, label: "storage engine", size: 15 });
    return f;
  });
  followers.forEach((f) => arrow([[leader.right + 6, f.cy < 300 ? 300 : 420], [f.x - 6, f.cy]]));
  note(800, 242, "AppendEntries");
  note(832, 452, "AppendEntries");

  note(leader.cx, 668, "a write commits once a majority of nodes hold it in their Raft log");

  box(960, 690, 36, 26, { fill: C.green, r: 6 });
  note(1006, 703, "built", { anchor: "start" });
  box(1080, 690, 36, 26, { dashed: true, r: 6 });
  note(1126, 703, "planned", { anchor: "start" });
});
