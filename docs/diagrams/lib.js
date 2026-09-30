import rough from "https://cdn.jsdelivr.net/npm/roughjs@4.6.6/bundled/rough.esm.js";

const INK = "#1e1e1e";
const MUTED = "#6b6b6b";

export const C = {
  gray: "#e9ecef",
  purple: "#d0bfff",
  yellow: "#ffec99",
  green: "#b2f2bb",
  blue: "#a5d8ff",
  red: "#ffc9c9",
  orange: "#ffd8a8",
  white: "#ffffff",
};

const SVG_NS = "http://www.w3.org/2000/svg";

function roundedRect(x, y, w, h, r) {
  return `M${x + r} ${y} L${x + w - r} ${y} Q${x + w} ${y} ${x + w} ${y + r} L${x + w} ${y + h - r} Q${x + w} ${y + h} ${x + w - r} ${y + h} L${x + r} ${y + h} Q${x} ${y + h} ${x} ${y + h - r} L${x} ${y + r} Q${x} ${y} ${x + r} ${y}`;
}

export async function diagram(width, height, draw) {
  await Promise.all([document.fonts.load("20px Virgil"), document.fonts.load("20px Cascadia")]);
  const svg = document.querySelector("svg");
  svg.setAttribute("width", width);
  svg.setAttribute("height", height);
  svg.setAttribute("viewBox", `0 0 ${width} ${height}`);

  const rc = rough.svg(svg);
  let seed = 1;
  const style = (extra = {}) => ({ roughness: 1, bowing: 1, strokeWidth: 1.8, stroke: INK, seed: seed++, ...extra });

  const text = (x, y, str, { size = 18, color = INK, anchor = "middle", font = "Virgil", middle = true } = {}) => {
    const lines = String(str).split("\n");
    const lineHeight = size * 1.3;
    const top = middle ? y - ((lines.length - 1) * lineHeight) / 2 : y;
    lines.forEach((line, i) => {
      const t = document.createElementNS(SVG_NS, "text");
      t.setAttribute("x", x);
      t.setAttribute("y", top + i * lineHeight);
      t.setAttribute("font-family", font);
      t.setAttribute("font-size", size);
      t.setAttribute("fill", color);
      t.setAttribute("text-anchor", anchor);
      t.setAttribute("dominant-baseline", "middle");
      t.textContent = line;
      svg.append(t);
    });
  };

  const box = (x, y, w, h, { fill = C.white, label, size = 18, font, dashed = false, r = 12 } = {}) => {
    svg.append(rc.path(roundedRect(x, y, w, h, r), style({ fill, fillStyle: "solid", strokeLineDash: dashed ? [8, 8] : undefined })));
    if (label !== undefined) text(x + w / 2, y + h / 2, label, { size, font });
    return { x, y, w, h, cx: x + w / 2, cy: y + h / 2, right: x + w, bottom: y + h };
  };

  const arrow = (points, { curve = false, dashed = false, color = INK } = {}) => {
    const opts = style({ stroke: color, strokeLineDash: dashed ? [8, 8] : undefined });
    svg.append(curve ? rc.curve(points, opts) : rc.linearPath(points, opts));
    const [px, py] = points[points.length - 2];
    const [ex, ey] = points[points.length - 1];
    const angle = Math.atan2(ey - py, ex - px);
    for (const side of [-1, 1]) {
      const a = angle + Math.PI - side * 0.45;
      svg.append(rc.line(ex, ey, ex + 16 * Math.cos(a), ey + 16 * Math.sin(a), style({ stroke: color })));
    }
  };

  const note = (x, y, str, opts = {}) => text(x, y, str, { size: 15, color: MUTED, ...opts });
  const title = (str) => text(40, 50, str, { size: 30, anchor: "start" });

  const bg = document.createElementNS(SVG_NS, "rect");
  bg.setAttribute("width", width);
  bg.setAttribute("height", height);
  bg.setAttribute("fill", "#ffffff");
  svg.append(bg);

  draw({ box, arrow, text, note, title, C });
  document.body.dataset.ready = "1";
}
