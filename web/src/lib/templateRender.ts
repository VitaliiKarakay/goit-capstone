const nameRe = /^[A-Za-z_][A-Za-z0-9_]*$/;
const PLACEHOLDER = /\[([A-Za-z_][A-Za-z0-9_]*)\]/g;

const maxField = 16 * 1024;

export function renderTemplate(text: string, vars: Record<string, string>): string {
  let out = "";
  let i = 0;
  while (i < text.length) {
    const ch = text[i];
    if (ch === "\\") {
      if (i + 1 >= text.length) throw new Error("invalid escape");
      const n = text[i + 1];
      if (n === "[" || n === "]" || n === "\\") {
        out += n;
        i += 2;
        continue;
      }
      throw new Error("invalid escape");
    }
    if (ch === "]") throw new Error("invalid template");
    if (ch === "[") {
      const end = text.indexOf("]", i + 1);
      if (end < 0) throw new Error("invalid template");
      const name = text.slice(i + 1, end);
      if (!nameRe.test(name)) throw new Error("invalid placeholder");
      if (!(name in vars)) throw new Error(`missing ${name}`);
      const v = vars[name];
      if (out.length + v.length > maxField) throw new Error("field too large");
      out += v;
      i = end + 1;
      continue;
    }
    if (out.length + 1 > maxField) throw new Error("field too large");
    out += ch;
    i++;
  }
  return out;
}

export function placeholdersIn(text: string): string[] {
  const names = new Set<string>();
  let m: RegExpExecArray | null;
  const re = new RegExp(PLACEHOLDER.source, "g");
  while ((m = re.exec(text)) !== null) {
    names.add(m[1]);
  }
  return [...names];
}