import { useState, useRef, useEffect } from 'react'

/**
 * Small ? circle that shows a tooltip on hover (desktop) or tap-toggle (mobile).
 * `content` can be a string or JSX.
 */
export default function InfoTooltip({ content, width = 280, placement = 'top' }) {
  const [visible, setVisible] = useState(false)
  const ref = useRef(null)

  // Close on outside click
  useEffect(() => {
    if (!visible) return
    function handler(e) {
      if (ref.current && !ref.current.contains(e.target)) setVisible(false)
    }
    document.addEventListener('mousedown', handler)
    return () => document.removeEventListener('mousedown', handler)
  }, [visible])

  return (
    <span
      ref={ref}
      onMouseEnter={() => setVisible(true)}
      onMouseLeave={() => setVisible(false)}
      style={{ verticalAlign: 'middle', display: 'inline-flex', alignItems: 'center', position: 'relative' }}
    >
      <button
        type="button"
        aria-label="More info"
        onClick={() => setVisible(v => !v)}
        style={{
          display: 'inline-flex',
          alignItems: 'center',
          justifyContent: 'center',
          width: 16,
          height: 16,
          borderRadius: '50%',
          border: '1px solid #52525b',
          background: 'transparent',
          color: '#a1a1aa',
          fontSize: 10,
          fontWeight: 700,
          cursor: 'pointer',
          lineHeight: 1,
          padding: 0,
          marginLeft: 5,
          flexShrink: 0,
        }}
      >
        i
      </button>

      {visible && (
        <div
          style={{
            position: 'absolute',
            ...(placement === 'bottom' ? { top: '120%' } : { bottom: '120%' }),
            left: '50%',
            transform: 'translateX(-50%)',
            zIndex: 50,
            width,
            background: '#27272a',
            border: '1px solid #3f3f46',
            borderRadius: 6,
            padding: '10px 12px',
            fontSize: 11,
            color: '#d4d4d8',
            lineHeight: 1.5,
            boxShadow: '0 4px 16px rgba(0,0,0,0.5)',
          }}
        >
          {content}
        </div>
      )}
    </span>
  )
}
