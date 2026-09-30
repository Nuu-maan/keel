import { diagram } from "./lib.js";

diagram(1400, 700, ({ box, arrow, text, note, title, C }) => {
  title("Wire protocol: many requests in flight on one connection");

  const frame = (y, fields, label) => {
    let x = 40;
    for (const [name, w, fill] of fields) {
      box(x, y, w, 56, { fill, label: name, font: "Cascadia", size: 14, r: 4 });
      x += w;
    }
    note(x + 20, y + 28, label, { anchor: "start", size: 17 });
  };
  frame(115, [
    ["length (4)", 150, C.orange],
    ["op (1)", 100, C.yellow],
    ["request id (8)", 190, C.purple],
    ["key len (uvarint)", 210, C.white],
    ["key", 150, C.green],
    ["value", 190, C.green],
  ], "request");
  frame(200, [
    ["length (4)", 150, C.orange],
    ["status (1)", 120, C.yellow],
    ["request id (8)", 190, C.purple],
    ["value", 190, C.green],
  ], "response");

  const calls = ["Get(a)  id=1", "Put(b)  id=2", "Get(c)  id=3"].map((label, i) =>
    box(40, 330 + i * 80, 200, 56, { fill: C.gray, label, font: "Cascadia", size: 15 }),
  );
  const conn = box(330, 340, 190, 200, { fill: C.blue, label: "one TCP\nconnection", size: 19 });
  calls.forEach((c) => arrow([[c.right + 6, c.cy], [conn.x - 6, c.cy]]));

  const handler = box(620, 310, 360, 260, { fill: C.purple });
  text(handler.cx, 340, "connection handler", { size: 19 });
  const read = box(645, 370, 310, 56, { fill: C.white, label: "read frame, decode request", size: 16 });
  const slots = box(645, 450, 310, 90, { fill: C.yellow, label: "≤ 256 requests in flight\nfull → stop reading,\nTCP pushes back", size: 16 });
  arrow([[conn.right + 6, read.cy], [read.x - 6, read.cy]]);
  arrow([[read.cx, read.bottom + 6], [slots.cx, slots.y - 6]]);

  const store = box(1110, 400, 260, 100, { fill: C.green, label: "storage engine\ngroup commit", size: 19 });
  arrow([[handler.right + 6, store.cy], [store.x - 6, store.cy]]);
  note(1045, 405, "one goroutine\nper request", { size: 14 });

  arrow([[handler.cx, handler.bottom + 6], [700, 640], [450, 640], [conn.cx, conn.bottom + 6]], { curve: true });
  note(560, 670, "responses return in completion order (2, 1, 3), matched by id");
  note(1240, 560, "frame > 16 MiB or malformed:\nconnection closed");
});
