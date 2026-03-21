export const COMPOUND_COLOURS = {
  SOFT: '#e8002d',
  MEDIUM: '#ffd800',
  HARD: '#afafaf',
  INTERMEDIATE: '#39b54a',
  WET: '#0067ff',
  UNKNOWN: '#888888',
}

export const COMPOUND_ORDER = ['SOFT', 'MEDIUM', 'HARD', 'INTERMEDIATE', 'WET', 'UNKNOWN']

export function compoundColour(compound) {
  return COMPOUND_COLOURS[(compound ?? '').toUpperCase()] ?? COMPOUND_COLOURS.UNKNOWN
}
