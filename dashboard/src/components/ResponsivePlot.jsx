import { useState, useEffect } from 'react'
import Plot from 'react-plotly.js'
import { useMobile } from '../hooks/useMobile'

/**
 * Drop-in replacement for react-plotly.js <Plot>.
 * Forces a full remount when the mobile breakpoint flips so Plotly
 * re-initialises with the correct container dimensions.
 *
 * The key update is deferred by one animation frame so the browser
 * finishes its CSS layout pass (container resize) before Plotly mounts
 * and measures dimensions. Without this, desktop→mobile fails because
 * the container hasn't yet shrunk when Plotly reads its bounding rect.
 */
export default function ResponsivePlot(props) {
  const isMobile = useMobile()
  const [plotKey, setPlotKey] = useState(isMobile ? 'mobile' : 'desktop')

  useEffect(() => {
    const id = requestAnimationFrame(() => {
      setPlotKey(isMobile ? 'mobile' : 'desktop')
    })
    return () => cancelAnimationFrame(id)
  }, [isMobile])

  return <Plot {...props} key={plotKey} />
}
