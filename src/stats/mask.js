export function applyMask(text, mask) {
  if (!text || !mask) return text;
  let output = text;
  const entries = Array.isArray(mask)
    ? mask
    : typeof mask === 'object'
    ? Object.entries(mask)
    : [];

  entries.forEach((entry) => {
    if (!entry) return;
    let source;
    let replacement;
    if (Array.isArray(entry)) {
      [source, replacement] = entry;
    } else if (entry && typeof entry === 'object') {
      source = entry.from ?? entry.search;
      replacement = entry.to ?? entry.replace ?? '';
    }
    if (!source || typeof source !== 'string') return;
    const replaceWith = replacement == null ? '' : String(replacement);
    const pattern = new RegExp(source.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'g');
    output = output.replace(pattern, replaceWith);
  });

  return output;
}
