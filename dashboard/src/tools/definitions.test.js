import { describe, it, expect } from 'vitest'
import {
  METRIC_META,
  formatToolsForProvider,
  extractGeminiToolCall,
  toGeminiContents,
  appendToolCallToHistory,
} from './definitions'

// ── METRIC_META ────────────────────────────────────────────────────────────────

describe('METRIC_META', () => {
  it('all lap_metrics entries point to the lap_metrics table', () => {
    const lapKeys = Object.entries(METRIC_META).filter(([k]) => k.endsWith('_lap') || k.startsWith('drs_') || k.startsWith('brake_'))
    for (const [k, v] of lapKeys) {
      expect(v.table, k).toBe('lap_metrics')
    }
  })

  it('stint metric entries point to the stint_metrics table', () => {
    const stintKeys = ['representative_pace_s', 'clean_air_pace_s', 'dirty_air_pace_s']
    for (const k of stintKeys) {
      expect(METRIC_META[k].table).toBe('stint_metrics')
    }
  })

  it('every entry has a non-empty label', () => {
    for (const [k, v] of Object.entries(METRIC_META)) {
      expect(v.label, k).toBeTruthy()
    }
  })
})

// ── formatToolsForProvider ─────────────────────────────────────────────────────

describe('formatToolsForProvider', () => {
  describe('anthropic', () => {
    const tools = formatToolsForProvider('anthropic')

    it('returns an array', () => {
      expect(Array.isArray(tools)).toBe(true)
    })

    it('each tool has name, description, and input_schema', () => {
      for (const t of tools) {
        expect(t.name).toBeTruthy()
        expect(t.description).toBeTruthy()
        expect(t.input_schema).toBeTruthy()
      }
    })

    it('aggregate_metric uses lowercase JSON Schema types', () => {
      const agg = tools.find(t => t.name === 'aggregate_metric')
      expect(agg.input_schema.type).toBe('object')
      expect(agg.input_schema.properties.metric.type).toBe('string')
    })
  })

  describe('gemini', () => {
    const tools = formatToolsForProvider('gemini')

    it('returns an array with a single functionDeclarations wrapper', () => {
      expect(Array.isArray(tools)).toBe(true)
      expect(tools).toHaveLength(1)
      expect(Array.isArray(tools[0].functionDeclarations)).toBe(true)
    })

    it('each declaration has name and description', () => {
      for (const d of tools[0].functionDeclarations) {
        expect(d.name).toBeTruthy()
        expect(d.description).toBeTruthy()
      }
    })

    it('aggregate_metric uses uppercase Gemini type names', () => {
      const decls = tools[0].functionDeclarations
      const agg = decls.find(d => d.name === 'aggregate_metric')
      expect(agg.parameters.type).toBe('OBJECT')
      expect(agg.parameters.properties.metric.type).toBe('STRING')
    })

    it('aggregation enum values are preserved', () => {
      const decls = tools[0].functionDeclarations
      const agg = decls.find(d => d.name === 'aggregate_metric')
      expect(agg.parameters.properties.aggregation.enum).toEqual(['avg', 'max', 'min', 'median', 'sum'])
    })
  })
})

// ── extractGeminiToolCall ──────────────────────────────────────────────────────

describe('extractGeminiToolCall', () => {
  it('returns null for non-tool chunks', () => {
    const chunk = { candidates: [{ content: { parts: [{ text: 'hello' }] } }] }
    expect(extractGeminiToolCall(chunk)).toBeNull()
  })

  it('returns null for empty/malformed input', () => {
    expect(extractGeminiToolCall(null)).toBeNull()
    expect(extractGeminiToolCall({})).toBeNull()
    expect(extractGeminiToolCall({ candidates: [] })).toBeNull()
  })

  it('extracts name and args from a functionCall part', () => {
    const chunk = {
      candidates: [{
        content: {
          parts: [{
            functionCall: { name: 'aggregate_metric', args: { metric: 'max_speed_kph_lap', aggregation: 'avg' } },
          }],
        },
      }],
    }
    const result = extractGeminiToolCall(chunk)
    expect(result).not.toBeNull()
    expect(result.name).toBe('aggregate_metric')
    expect(result.args).toEqual({ metric: 'max_speed_kph_lap', aggregation: 'avg' })
  })

  it('returns empty args object when functionCall.args is missing', () => {
    const chunk = {
      candidates: [{ content: { parts: [{ functionCall: { name: 'aggregate_metric' } }] } }],
    }
    const result = extractGeminiToolCall(chunk)
    expect(result.args).toEqual({})
  })
})

// ── toGeminiContents ───────────────────────────────────────────────────────────

describe('toGeminiContents', () => {
  it('maps user role to "user"', () => {
    const msgs = [{ role: 'user', content: 'hello' }]
    const out = toGeminiContents(msgs)
    expect(out[0].role).toBe('user')
  })

  it('maps assistant role to "model"', () => {
    const msgs = [{ role: 'assistant', content: 'hi' }]
    const out = toGeminiContents(msgs)
    expect(out[0].role).toBe('model')
  })

  it('wraps plain content string in a text part', () => {
    const msgs = [{ role: 'user', content: 'hello' }]
    const out = toGeminiContents(msgs)
    expect(out[0].parts).toEqual([{ text: 'hello' }])
  })

  it('preserves pre-formatted parts (tool response round-trip)', () => {
    const parts = [{ functionResponse: { name: 'foo', response: { result: 'bar' } } }]
    const msgs = [{ role: 'user', parts }]
    const out = toGeminiContents(msgs)
    expect(out[0].parts).toBe(parts)
  })
})

// ── appendToolCallToHistory ────────────────────────────────────────────────────

describe('appendToolCallToHistory', () => {
  const history = [{ role: 'user', content: 'who is fastest?' }]
  const toolCall = { id: 'tc_001', name: 'aggregate_metric', args: { metric: 'max_speed_kph_lap', aggregation: 'max' } }
  const result = 'max(max_speed_kph_lap) per driver: 1. VER: 345.00 (n=20)'

  describe('anthropic', () => {
    const updated = appendToolCallToHistory(history, toolCall, result, 'anthropic')

    it('appends two messages', () => {
      expect(updated).toHaveLength(history.length + 2)
    })

    it('first appended is assistant with tool_use content', () => {
      const msg = updated[updated.length - 2]
      expect(msg.role).toBe('assistant')
      expect(msg.content[0].type).toBe('tool_use')
      expect(msg.content[0].id).toBe('tc_001')
      expect(msg.content[0].name).toBe('aggregate_metric')
    })

    it('second appended is user with tool_result content', () => {
      const msg = updated[updated.length - 1]
      expect(msg.role).toBe('user')
      expect(msg.content[0].type).toBe('tool_result')
      expect(msg.content[0].tool_use_id).toBe('tc_001')
      expect(msg.content[0].content).toBe(result)
    })

    it('does not mutate original history', () => {
      expect(history).toHaveLength(1)
    })
  })

  describe('gemini', () => {
    const geminiHistory = toGeminiContents(history)
    const updated = appendToolCallToHistory(geminiHistory, toolCall, result, 'gemini')

    it('appends two messages', () => {
      expect(updated).toHaveLength(geminiHistory.length + 2)
    })

    it('first appended is model with functionCall part', () => {
      const msg = updated[updated.length - 2]
      expect(msg.role).toBe('model')
      expect(msg.parts[0].functionCall.name).toBe('aggregate_metric')
      expect(msg.parts[0].functionCall.args).toEqual(toolCall.args)
    })

    it('second appended is user with functionResponse part', () => {
      const msg = updated[updated.length - 1]
      expect(msg.role).toBe('user')
      expect(msg.parts[0].functionResponse.name).toBe('aggregate_metric')
      expect(msg.parts[0].functionResponse.response.result).toBe(result)
    })
  })
})
