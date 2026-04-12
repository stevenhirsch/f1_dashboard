// Tool definitions and provider translation layer.
// Tools are defined once in neutral (JSON Schema) format and translated per-provider at call time.

export const METRIC_META = {
  // lap_metrics — per-lap telemetry derived values
  max_speed_kph_lap:                { table: 'lap_metrics', label: 'max speed (kph)' },
  coasting_ratio_lap:               { table: 'lap_metrics', label: 'coasting fraction (throttle<1%, brake=0)' },
  coasting_distance_m_lap:          { table: 'lap_metrics', label: 'coasting distance per lap (m)' },
  full_throttle_pct_lap:            { table: 'lap_metrics', label: 'full-throttle fraction (throttle≥99%)' },
  throttle_brake_overlap_ratio_lap: { table: 'lap_metrics', label: 'trail-braking overlap ratio (brake>0 AND throttle≥10%)' },
  throttle_input_variance_lap:      { table: 'lap_metrics', label: 'throttle input variance — higher = rougher inputs' },
  mean_peak_decel_g_lap:            { table: 'lap_metrics', label: 'mean peak braking G across all brake zones' },
  max_linear_deceleration_g_lap:    { table: 'lap_metrics', label: 'single-lap peak deceleration G' },
  max_linear_acceleration_g_lap:    { table: 'lap_metrics', label: 'single-lap peak acceleration G' },
  drs_activation_count:             { table: 'lap_metrics', label: 'DRS activations per lap' },
  drs_distance_m:                   { table: 'lap_metrics', label: 'distance driven with DRS open (m)' },
  brake_zone_count_lap:             { table: 'lap_metrics', label: 'distinct braking events per lap' },
  // stint_metrics — per-stint aggregated pace values
  representative_pace_s:  { table: 'stint_metrics', label: 'median race pace, racing laps only (s)' },
  clean_air_pace_s:       { table: 'stint_metrics', label: 'mean pace in clean air — gap_ahead > 2s (s)' },
  dirty_air_pace_s:       { table: 'stint_metrics', label: 'mean pace in dirty air — gap_ahead ≤ 2s (s)' },
}

const METRIC_DESCRIPTIONS = Object.entries(METRIC_META)
  .map(([k, v]) => `${k} (${v.label})`)
  .join('; ')

const TOOL_DEFS = [
  {
    name: 'aggregate_metric',
    description: `Compute aggregated statistics for any per-lap or per-stint telemetry/pace metric. Use this when the user asks for averages, maximums, rankings, or comparisons that aren't directly in the context — e.g. "who has the highest average top speed", "what's Hamilton's coasting ratio", "which race had the most DRS activations". Available metrics: ${METRIC_DESCRIPTIONS}`,
    parameters: {
      type: 'object',
      properties: {
        metric: {
          type: 'string',
          description: `Metric column to aggregate. Must be one of: ${Object.keys(METRIC_META).join(', ')}`,
        },
        aggregation: {
          type: 'string',
          enum: ['avg', 'max', 'min', 'median', 'sum'],
          description: 'Aggregation function to apply across the filtered rows',
        },
        group_by: {
          type: 'string',
          enum: ['driver', 'race', 'none'],
          description: 'Group results by driver (default), by race, or return a single scalar value',
        },
        driver: {
          type: 'string',
          description: 'Filter to one driver — use their 3-letter acronym (e.g. HAM, VER, NOR)',
        },
        race: {
          type: 'string',
          description: 'Filter to one race — use circuit short name or location (e.g. Bahrain, Silverstone, Monaco)',
        },
      },
      required: ['metric', 'aggregation'],
    },
  },
]

// ── Provider formatting ────────────────────────────────────────────────────────

function geminiType(t) {
  return { string: 'STRING', number: 'NUMBER', integer: 'INTEGER', boolean: 'BOOLEAN', object: 'OBJECT', array: 'ARRAY' }[t] ?? 'STRING'
}

function toGeminiSchema(schema) {
  if (!schema) return {}
  const out = { type: geminiType(schema.type) }
  if (schema.description) out.description = schema.description
  if (schema.enum)         out.enum = schema.enum
  if (schema.properties) {
    out.properties = {}
    for (const [k, v] of Object.entries(schema.properties)) out.properties[k] = toGeminiSchema(v)
  }
  if (schema.required) out.required = schema.required
  return out
}

// Returns provider-specific tools array to include in the API request body.
export function formatToolsForProvider(provider) {
  if (provider === 'gemini') {
    return [{
      functionDeclarations: TOOL_DEFS.map(t => ({
        name:        t.name,
        description: t.description,
        parameters:  toGeminiSchema(t.parameters),
      })),
    }]
  }
  // Anthropic
  return TOOL_DEFS.map(t => ({
    name:         t.name,
    description:  t.description,
    input_schema: t.parameters,
  }))
}

// ── Stream helpers ─────────────────────────────────────────────────────────────

// Extracts a Gemini function call from a parsed SSE chunk. Returns { name, args } or null.
export function extractGeminiToolCall(parsed) {
  const parts = parsed?.candidates?.[0]?.content?.parts ?? []
  for (const part of parts) {
    if (part.functionCall) return { name: part.functionCall.name, args: part.functionCall.args ?? {}, id: null }
  }
  return null
}

// ── History builders ──────────────────────────────────────────────────────────

// Convert neutral {role, content} messages to Gemini contents array.
export function toGeminiContents(messages) {
  return messages.map(m => ({
    role:  m.role === 'assistant' ? 'model' : 'user',
    parts: m.parts ?? [{ text: m.content ?? '' }],
  }))
}

// Append a tool call + result to the provider-formatted request history.
export function appendToolCallToHistory(history, toolCall, resultText, provider) {
  if (provider === 'gemini') {
    return [
      ...history,
      { role: 'model', parts: [{ functionCall: { name: toolCall.name, args: toolCall.args } }] },
      { role: 'user',  parts: [{ functionResponse: { name: toolCall.name, response: { result: resultText } } }] },
    ]
  }
  // Anthropic
  return [
    ...history,
    { role: 'assistant', content: [{ type: 'tool_use', id: toolCall.id, name: toolCall.name, input: toolCall.args }] },
    { role: 'user',      content: [{ type: 'tool_result', tool_use_id: toolCall.id, content: resultText }] },
  ]
}
