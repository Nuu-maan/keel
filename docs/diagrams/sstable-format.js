import { diagram } from "./lib.js";

diagram(1400, 600, ({ box, arrow, note, title, C }) => {
  title("SSTable layout: one pread per lookup");

  const seg = (x, w, label, fill, opts = {}) => box(x, 130, w, 70, { fill, label, r: 4, size: 17, ...opts });
  const block0 = seg(40, 170, "block 0", C.green);
  seg(210, 170, "block 1", C.green);
  seg(380, 80, "…", C.white);
  seg(460, 170, "block n", C.green);
  const filter = seg(630, 170, "filter", C.yellow);
  const index = seg(800, 170, "index", C.white);
  const footer = seg(970, 170, "footer", C.orange);
  note(1170, 165, "← 40 bytes, read first", { anchor: "start" });

  const detail = (x, w, label) => box(x, 300, w, 170, { fill: C.white, label, font: "Cascadia", size: 14 });
  const d0 = detail(40, 400, "entry: key len · key · op · val len · val\nentry: ...\nentry: ...\ncrc32c");
  const d1 = detail(470, 270, "10 bits per key\n7 probes, double hashing\nFNV-1a 64\n~0.9% false positives");
  const d2 = detail(770, 290, "per block:\nlast key\noffset · length\nloaded on open");
  const d3 = detail(1090, 270, "filter: off · len · crc\nindex:  off · len · crc\nmagic KEELSST2");

  for (const [from, to] of [[block0, d0], [filter, d1], [index, d2], [footer, d3]]) {
    arrow([[from.cx, from.bottom + 6], [to.cx, to.y - 6]], { dashed: true });
  }

  note(700, 520, "lookup: filter says maybe → binary search the index → pread exactly one block → verify its checksum");
  note(700, 550, "every section carries a CRC32C; a mismatch is ErrCorrupt, never bad data");
});
