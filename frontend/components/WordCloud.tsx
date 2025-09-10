"use client"

import React, { useEffect, useRef, useState, useCallback, useMemo } from 'react';

interface WordCloudProps {
  data: Array<{
    text: string;
    value: number;
  }>;
  width?: number;
  height?: number;
  className?: string;
  onWordClick?: (word: string, value: number) => void;
}

interface WordPosition {
  text: string;
  value: number;
  x: number;
  y: number;
  width: number;
  height: number;
  fontSize: number;
  color: string;
}

const WordCloud: React.FC<WordCloudProps> = ({ 
  data, 
  width = 1200, 
  height = 600, 
  className = "",
  onWordClick
}) => {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  const [canvasSize, setCanvasSize] = useState({ width, height });
  const [contentBounds, setContentBounds] = useState({ minX: 0, minY: 0, maxX: 0, maxY: 0 });
  const [placedWords, setPlacedWords] = useState<WordPosition[]>([]);
  const [zoom, setZoom] = useState(1);
  const [pan, setPan] = useState({ x: 0, y: 0 });
  const [isDragging, setIsDragging] = useState(false);
  const [dragStart, setDragStart] = useState({ x: 0, y: 0 });
  const [hoveredWord, setHoveredWord] = useState<WordPosition | null>(null);

  // Handle responsive sizing based on container width, not content bounds
  useEffect(() => {
    const updateCanvasDimensions = () => {
      if (containerRef.current) {
        const container = containerRef.current;
        const containerWidth = container.clientWidth;
        const newWidth = Math.min(containerWidth, width);
        const newHeight = (newWidth / width) * height;
        
        setCanvasSize({ 
          width: Math.max(newWidth, 400), 
          height: Math.max(newHeight, 300) 
        });
      }
    };

    updateCanvasDimensions();
    window.addEventListener('resize', updateCanvasDimensions);
    return () => window.removeEventListener('resize', updateCanvasDimensions);
  }, [width, height]); // Only depend on props, not state

  const colors = useMemo(() => [
    '#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FFEAA7', '#DDA0DD', '#98D8C8', '#F7DC6F',
    '#FF9F43', '#10AC84', '#EE5A24', '#0984E3', '#6C5CE7', '#A29BFE', '#FD79A8', '#FDCB6E',
    '#E17055', '#00B894', '#00CEC9', '#74B9FF', '#A29BFE', '#FD79A8', '#FDCB6E', '#E17055',
    '#FF7675', '#00B894', '#00CEC9', '#74B9FF', '#A29BFE', '#FD79A8', '#FDCB6E', '#E17055',
    '#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FFEAA7', '#DDA0DD', '#98D8C8', '#F7DC6F'
  ], []);

  const drawWord = useCallback((
    ctx: CanvasRenderingContext2D, 
    word: WordPosition, 
    isHovered: boolean = false
  ) => {
    // Use a more readable font with better weight
    ctx.font = `700 ${word.fontSize}px 'Segoe UI', Arial, sans-serif`;
    ctx.textAlign = 'center';
    ctx.textBaseline = 'middle';
    
    // Fill text with vibrant color - no glow effects
    ctx.fillStyle = isHovered ? '#FFFFFF' : word.color;
    ctx.fillText(word.text, word.x, word.y);
  }, []);

  const checkCollision = useCallback((
    newWord: WordPosition, 
    existingWords: WordPosition[], 
    padding: number
  ) => {
    for (const existingWord of existingWords) {
      // Check for overlap using bounding boxes
      if (
        newWord.x - newWord.width / 2 < existingWord.x + existingWord.width / 2 + padding &&
        newWord.x + newWord.width / 2 + padding > existingWord.x - existingWord.width / 2 &&
        newWord.y - newWord.height / 2 < existingWord.y + existingWord.height / 2 + padding &&
        newWord.y + newWord.height / 2 + padding > existingWord.y - existingWord.height / 2
      ) {
        return true; // Collision detected
      }
    }
    return false; // No collision
  }, []);

  // Memoize the placeWords function to prevent infinite loops
  const placeWords = useMemo(() => {
    return () => {
      if (!canvasRef.current || data.length === 0) return;

      const canvas = canvasRef.current;
      const ctx = canvas.getContext('2d');
      if (!ctx) return;

      ctx.clearRect(0, 0, canvasSize.width, canvasSize.height);

      const wordsToPlace = [...data].sort((a, b) => b.value - a.value); // Sort by value descending
      const newPlacedWords: WordPosition[] = [];

      // Adjust font size range based on number of words - optimized for 70+ words
      const minFontSize = Math.max(10, Math.min(14, 40 / Math.sqrt(data.length)));
      const maxFontSize = Math.max(16, Math.min(32, 60 / Math.sqrt(data.length)));
      const padding = Math.max(3, Math.min(8, 15 / Math.sqrt(data.length))); // Reduced padding for more words
      const maxAttempts = 20000; // Increased attempts for more words

      // Calculate font size scale
      const values = data.map(d => d.value);
      const minValue = Math.min(...values);
      const maxValue = Math.max(...values);

      const getFontSize = (value: number) => {
        if (minValue === maxValue) return (minFontSize + maxFontSize) / 2;
        return minFontSize + (value - minValue) / (maxValue - minValue) * (maxFontSize - minFontSize);
      };

      // Compact spiral positioning for better space utilization
      const centerX = canvasSize.width / 2;
      const centerY = canvasSize.height / 2;
      const maxRadius = Math.min(canvasSize.width, canvasSize.height) / 3; // Reduced radius for compact layout

      for (let i = 0; i < wordsToPlace.length; i++) {
        const wordData = wordsToPlace[i];
        const fontSize = getFontSize(wordData.value);
        ctx.font = `normal ${fontSize}px Arial, sans-serif`; // Set font for measurement
        const textMetrics = ctx.measureText(wordData.text);
        const textWidth = textMetrics.width;
        const textHeight = fontSize * 1.2; // Reduced height multiplier

        let placed = false;
        let attempts = 0;

        // Use multiple spiral patterns for better distribution
        const spiralCount = Math.min(8, Math.ceil(wordsToPlace.length / 10));
        const spiralIndex = i % spiralCount;
        const angleOffset = (spiralIndex * 2 * Math.PI) / spiralCount;
        const radiusStep = 1.2 + spiralIndex * 0.2;
        const angleStep = 0.12 + spiralIndex * 0.03;

        while (!placed && attempts < maxAttempts) {
          const angle = angleOffset + attempts * angleStep;
          const radius = attempts * radiusStep;

          const x = centerX + radius * Math.cos(angle);
          const y = centerY + radius * Math.sin(angle);

          const newWord: WordPosition = {
            text: wordData.text,
            value: wordData.value,
            x: x,
            y: y,
            width: textWidth,
            height: textHeight,
            fontSize: fontSize,
            color: colors[i % colors.length]
          };

          // Check if word is within canvas bounds with margin
          const margin = 15;
          const isWithinBounds = 
            newWord.x - newWord.width / 2 >= margin &&
            newWord.x + newWord.width / 2 <= canvasSize.width - margin &&
            newWord.y - newWord.height / 2 >= margin &&
            newWord.y + newWord.height / 2 <= canvasSize.height - margin;

          if (isWithinBounds && !checkCollision(newWord, newPlacedWords, padding)) {
            newPlacedWords.push(newWord);
            placed = true;
          }

          attempts++;
        }

        // If still not placed, try grid-based positioning as fallback
        if (!placed) {
          const gridSize = Math.max(textWidth, textHeight) + padding * 1.2;
          const cols = Math.floor((canvasSize.width - 30) / gridSize);
          const rows = Math.floor((canvasSize.height - 30) / gridSize);
          
          for (let row = 0; row < rows && !placed; row++) {
            for (let col = 0; col < cols && !placed; col++) {
              const x = 15 + col * gridSize + gridSize / 2;
              const y = 15 + row * gridSize + gridSize / 2;

              const gridWord: WordPosition = {
                text: wordData.text,
                value: wordData.value,
                x: x,
                y: y,
                width: textWidth,
                height: textHeight,
                fontSize: fontSize,
                color: colors[i % colors.length]
              };

              if (!checkCollision(gridWord, newPlacedWords, padding)) {
                newPlacedWords.push(gridWord);
                placed = true;
              }
            }
          }
        }

        // Final fallback: place word even if it overlaps slightly
        if (!placed) {
          // Find the best available position with minimal overlap
          let bestPosition = { x: centerX, y: centerY };
          let minOverlap = Infinity;

          for (let attempt = 0; attempt < 1000; attempt++) {
            const angle = Math.random() * 2 * Math.PI;
            const radius = Math.random() * maxRadius;
            const x = centerX + radius * Math.cos(angle);
            const y = centerY + radius * Math.sin(angle);

            const testWord: WordPosition = {
              text: wordData.text,
              value: wordData.value,
              x: x,
              y: y,
              width: textWidth,
              height: textHeight,
              fontSize: fontSize,
              color: colors[i % colors.length]
            };

            // Calculate overlap with existing words
            let totalOverlap = 0;
            for (const existingWord of newPlacedWords) {
              const overlapX = Math.max(0, Math.min(testWord.x + testWord.width/2, existingWord.x + existingWord.width/2) - 
                                        Math.max(testWord.x - testWord.width/2, existingWord.x - existingWord.width/2));
              const overlapY = Math.max(0, Math.min(testWord.y + testWord.height/2, existingWord.y + existingWord.height/2) - 
                                        Math.max(testWord.y - testWord.height/2, existingWord.y - existingWord.height/2));
              totalOverlap += overlapX * overlapY;
            }

            if (totalOverlap < minOverlap) {
              minOverlap = totalOverlap;
              bestPosition = { x, y };
            }
          }

          const fallbackWord: WordPosition = {
            text: wordData.text,
            value: wordData.value,
            x: bestPosition.x,
            y: bestPosition.y,
            width: textWidth,
            height: textHeight,
            fontSize: fontSize,
            color: colors[i % colors.length]
          };

          newPlacedWords.push(fallbackWord);
          placed = true;
        }
      }
      
      // Calculate content bounds for responsive sizing
      if (newPlacedWords.length > 0) {
        const bounds = newPlacedWords.reduce((acc, word) => ({
          minX: Math.min(acc.minX, word.x - word.width / 2),
          minY: Math.min(acc.minY, word.y - word.height / 2),
          maxX: Math.max(acc.maxX, word.x + word.width / 2),
          maxY: Math.max(acc.maxY, word.y + word.height / 2)
        }), {
          minX: Infinity,
          minY: Infinity,
          maxX: -Infinity,
          maxY: -Infinity
        });
        setContentBounds(bounds);
      } else {
        setContentBounds({ minX: 0, minY: 0, maxX: 0, maxY: 0 });
      }
      
      setPlacedWords(newPlacedWords);
        
        // Log placement statistics
        console.log(`WordCloud: Attempted to place ${wordsToPlace.length} words, successfully placed ${newPlacedWords.length} words`);
        if (newPlacedWords.length < wordsToPlace.length) {
          console.log(`WordCloud: ${wordsToPlace.length - newPlacedWords.length} words could not be placed optimally`);
        }
      };
    }, [data, canvasSize, checkCollision, colors]);

  // Redraw words when placedWords, canvasSize, zoom, pan, or hoveredWord changes
  useEffect(() => {
    if (!canvasRef.current) return;
    const canvas = canvasRef.current;
    const ctx = canvas.getContext('2d');
    if (!ctx) return;

    ctx.clearRect(0, 0, canvasSize.width, canvasSize.height);
    
    // Apply zoom and pan transformations
    ctx.save();
    ctx.translate(pan.x, pan.y);
    ctx.scale(zoom, zoom);
    
    placedWords.forEach(word => drawWord(ctx, word, word === hoveredWord));
    ctx.restore();
  }, [placedWords, canvasSize, zoom, pan, hoveredWord, drawWord]);

  // Initial placement and re-placement on data/size change
  useEffect(() => {
    placeWords();
  }, [placeWords]);



  // Handle pan with mouse drag
  const handleMouseDown = useCallback((event: React.MouseEvent<HTMLCanvasElement>) => {
    setIsDragging(true);
    setDragStart({ x: event.clientX - pan.x, y: event.clientY - pan.y });
  }, [pan]);

  // Handle click events with zoom and pan transformation
  const handleClick = useCallback((event: React.MouseEvent<HTMLCanvasElement>) => {
    if (!canvasRef.current || !onWordClick || isDragging) return;

    const canvas = canvasRef.current;
    const rect = canvas.getBoundingClientRect();
    const scaleX = canvas.width / rect.width;
    const scaleY = canvas.height / rect.height;

    const mouseX = (event.clientX - rect.left) * scaleX;
    const mouseY = (event.clientY - rect.top) * scaleY;

    for (const word of placedWords) {
      // Transform coordinates for zoom and pan
      const transformedX = (word.x * zoom) + pan.x;
      const transformedY = (word.y * zoom) + pan.y;
      const transformedWidth = word.width * zoom;
      const transformedHeight = word.height * zoom;
      
      if (
        mouseX >= transformedX - transformedWidth / 2 &&
        mouseX <= transformedX + transformedWidth / 2 &&
        mouseY >= transformedY - transformedHeight / 2 &&
        mouseY <= transformedY + transformedHeight / 2
      ) {
        onWordClick(word.text, word.value);
        return;
      }
    }
  }, [placedWords, onWordClick, isDragging, zoom, pan]);

  const handleMouseMove = useCallback((event: React.MouseEvent<HTMLCanvasElement>) => {
    if (!canvasRef.current) return;

    const canvas = canvasRef.current;
    const ctx = canvas.getContext('2d');
    if (!ctx) return;

    const rect = canvas.getBoundingClientRect();
    const scaleX = canvas.width / rect.width;
    const scaleY = canvas.height / rect.height;

    const mouseX = (event.clientX - rect.left) * scaleX;
    const mouseY = (event.clientY - rect.top) * scaleY;

    // Handle panning
    if (isDragging) {
      const newPanX = event.clientX - dragStart.x;
      const newPanY = event.clientY - dragStart.y;
      setPan({ x: newPanX, y: newPanY });
      return;
    }

    // Handle hover effects
    let currentHovered: WordPosition | null = null;
    for (const word of placedWords) {
      // Transform coordinates for zoom and pan
      const transformedX = (word.x * zoom) + pan.x;
      const transformedY = (word.y * zoom) + pan.y;
      const transformedWidth = word.width * zoom;
      const transformedHeight = word.height * zoom;
      
      if (
        mouseX >= transformedX - transformedWidth / 2 &&
        mouseX <= transformedX + transformedWidth / 2 &&
        mouseY >= transformedY - transformedHeight / 2 &&
        mouseY <= transformedY + transformedHeight / 2
      ) {
        currentHovered = word;
        break;
      }
    }

    if (currentHovered !== hoveredWord) {
      setHoveredWord(currentHovered);
    }
  }, [placedWords, hoveredWord, canvasSize, drawWord, isDragging, dragStart, pan, zoom]);

  const handleMouseUp = useCallback(() => {
    setIsDragging(false);
  }, []);

  const handleMouseOut = useCallback(() => {
    setHoveredWord(null);
  }, []);

  // Reset zoom and pan function
  const resetView = useCallback(() => {
    setZoom(1);
    setPan({ x: 0, y: 0 });
  }, []);

  return (
    <div 
      ref={containerRef} 
      className={`relative w-full ${className}`}
      style={{ 
        height: `${canvasSize.height}px`, // Fixed rectangle height
        maxWidth: '100%',
        display: 'flex',
        justifyContent: 'center',
        alignItems: 'center',
        overflow: 'hidden'
      }}
    >
      {/* Zoom controls */}
      <div className="absolute top-4 right-4 z-10 flex flex-col gap-2">
        <button
          onClick={() => setZoom(prev => Math.min(3, prev + 0.2))}
          className="bg-gray-800 text-white p-2 rounded-lg hover:bg-gray-700 transition-colors"
          title="Zoom In"
        >
          +
        </button>
        <button
          onClick={() => setZoom(prev => Math.max(0.5, prev - 0.2))}
          className="bg-gray-800 text-white p-2 rounded-lg hover:bg-gray-700 transition-colors"
          title="Zoom Out"
        >
          −
        </button>
        <button
          onClick={resetView}
          className="bg-gray-800 text-white p-2 rounded-lg hover:bg-gray-700 transition-colors"
          title="Reset View"
        >
          ↺
        </button>
      </div>
      
      {/* Zoom indicator */}
      <div className="absolute bottom-4 left-4 z-10 bg-gray-800 text-white px-3 py-1 rounded-lg text-sm">
        {Math.round(zoom * 100)}%
      </div>

      <canvas
        ref={canvasRef}
        width={canvasSize.width}
        height={canvasSize.height}
        className="block rounded-lg"
        onClick={handleClick}
        onMouseMove={handleMouseMove}
        onMouseDown={handleMouseDown}
        onMouseUp={handleMouseUp}
        onMouseOut={handleMouseOut}
        style={{ 
          cursor: isDragging ? 'grabbing' : hoveredWord ? 'pointer' : 'grab',
          maxWidth: '100%',
          height: 'auto'
        }}
      />
    </div>
  );
};

export default WordCloud; 