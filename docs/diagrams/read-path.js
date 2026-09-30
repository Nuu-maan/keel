import { diagram } from "./lib.js";

diagram(1400, 640, ({ box, arrow, text, note, title, C }) => {
  title("Read path: newest data first, bloom filter before disk");

  const get = box(40, 300, 150, 60, { fill: C.gray, label: "Get(k)", font: "Cascadia", size: 17 });
  const mem = box(250, 290, 170, 80, { fill: C.blue, label: "memtable" });
  arrow([[get.right + 6, get.cy], [mem.x - 6, mem.cy]]);
  note(mem.cx, mem.y - 22, "hit → return");

  const tables = ["000012.sst · newest", "000008.sst", "000003.sst · oldest"].map((name, i) => {
    const x = 500 + i * 310;
    text(x + 105, 150, name, { size: 17, font: "Cascadia" });
    const bloom = box(x, 180, 210, 66, { fill: C.yellow, label: "bloom filter\nin memory", size: 16 });
    const index = box(x, 300, 210, 66, { fill: C.white, label: "block index\nbinary search", size: 16 });
    const block = box(x, 420, 210, 66, { fill: C.green, label: "one ~4 KiB block\none pread", size: 16 });
    arrow([[bloom.cx, bloom.bottom + 6], [index.cx, index.y - 6]]);
    arrow([[index.cx, index.bottom + 6], [block.cx, block.y - 6]]);
    note(bloom.cx + 14, 273, "maybe", { anchor: "start" });
    return bloom;
  });

  arrow([[mem.right + 6, mem.cy], [tables[0].x - 6, tables[0].cy]]);
  note(452, 250, "miss", { anchor: "end" });
  tables.slice(1).forEach((b, i) => {
    arrow([[tables[i].right + 6, b.cy], [b.x - 6, b.cy]]);
    note(b.x - 50, b.cy + 34, "no: skip\n~99%");
  });

  note(800, 540, "stop at the first match · a tombstone means ErrNotFound, even if older tables hold a value");
  note(800, 570, "a key a table doesn't hold costs no disk read about 99% of the time");
});
