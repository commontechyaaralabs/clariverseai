"use client";

import React, { useState, useEffect } from 'react';
import { ChevronRight, Brain, Database, Settings, Users, ArrowRight, Mail, MessageSquare, Ticket, TrendingUp, Shield, Zap, Phone, Share2 } from 'lucide-react';
import Sidebar from '@/components/Sidebar/Sidebar';
import { Header } from '@/components/Header/Header';
import { useSession } from "next-auth/react";
import { useRouter } from "next/navigation";
import { AuthGuard } from "@/components/AuthGuard";


// Interface for wave animation configuration
interface WaveOptions {
  resize?: boolean;
  rotation?: number;
  waves?: number;
  width?: number;
  hue?: [number, number];
  amplitude?: number;
  preload?: boolean;
  speed?: [number, number];
  debug?: boolean;
  fps?: boolean;
  spacing?: number;
  opacity?: { start: number; end: number };
  backgroundColor?: string;
  startColor?: string;
  endColor?: string;
  speedMultiplier?: number;
}

// Interface for ColorManager
interface ColorManager {
  startColor: string;
  endColor: string;
  START_OPACITY: number;
  END_OPACITY: number;
  updateColors: (newStart: string, newEnd: string) => void;
  getInterpolatedColor: (progress: number) => string;
}

// Forward declaration of Waves class for use in Wave
class Waves {
  options: WaveOptions;
  waves: Wave[];
  holder: HTMLElement | null;
  canvas: HTMLCanvasElement;
  ctx: CanvasRenderingContext2D | null;
  hue: number;
  hueFw: boolean;
  color: string;
  scale: number;
  width: number;
  height: number;
  radius: number;
  centerX: number;
  centerY: number;

  constructor(holder: string, options: WaveOptions) {
    this.options = Object.assign({}, {
      resize: true,
      rotation: 45,
      waves: 5,
      width: 100,
      hue: [11, 14],
      amplitude: 0.5,
      preload: true,
      speed: [0.004, 0.008],
      debug: false,
      fps: false,
      spacing: 3,
      opacity: { start: 1.0, end: 0 }
    }, options);

    this.waves = [];
    this.holder = document.querySelector(holder);
    this.canvas = document.createElement("canvas");
    this.ctx = this.canvas.getContext("2d");
    this.holder?.appendChild(this.canvas);

    this.hue = this.options.hue![0];
    this.hueFw = true;
    this.color = '';
    this.scale = 0;
    this.width = 0;
    this.height = 0;
    this.radius = 0;
    this.centerX = 0;
    this.centerY = 0;

    this.resize();
    this.init(this.options.preload!);

    const resizeObserver = new ResizeObserver(() => {
      this.resize();
    });
    if (this.holder) {
      resizeObserver.observe(this.holder);
    }

    if (this.options.resize) {
      window.addEventListener("resize", () => this.resize(), false);
    }
  }

  init(preload: boolean) {
    this.waves = [];
    for (let i = 0; i < this.options.waves!; i++) {
      this.waves[i] = new Wave(this);
    }
    if (preload) this.preload();
  }

  preload() {
    for (let i = 0; i < this.options.waves!; i++) {
      this.updateColor();
      for (let j = 0; j < this.options.width!; j++) {
        this.waves[i].update();
      }
    }
  }

  render() {
    if (!this.ctx) return;

    this.updateColor();
    this.clear();

    if (this.options.debug) {
      this.ctx.beginPath();
      this.ctx.strokeStyle = "#f00";
      this.ctx.arc(this.centerX, this.centerY, this.radius, 0, Math.PI * 2);
      this.ctx.stroke();
    }

    this.waves.forEach((wave) => {
      wave.update();
      wave.draw();
    });
  }

  animate() {
    this.render();
    window.requestAnimationFrame(this.animate.bind(this));
  }

  clear() {
    if (!this.ctx) return;
    this.ctx.clearRect(0, 0, this.width, this.height);
  }

  resize() {
    if (!this.holder || !this.ctx) return;

    const width = this.holder.offsetWidth;
    const height = this.holder.offsetHeight;

    this.scale = window.devicePixelRatio || 1;
    this.width = width * this.scale;
    this.height = height * this.scale;

    this.canvas.width = this.width;
    this.canvas.height = this.height;
    this.canvas.style.width = `${width}px`;
    this.canvas.style.height = `${height}px`;

    this.radius = Math.sqrt(Math.pow(this.width, 2) + Math.pow(this.height, 2)) / 2;
    this.centerX = this.width / 2;
    this.centerY = this.height / 2;
  }

  updateColor() {
    if (!this.ctx) return;

    this.hue += this.hueFw ? 0.01 : -0.01;

    if (this.hue > this.options.hue![1] && this.hueFw) {
      this.hue = this.options.hue![1];
      this.hueFw = false;
    } else if (this.hue < this.options.hue![0] && !this.hueFw) {
      this.hue = this.options.hue![0];
      this.hueFw = true;
    }

    const a = Math.floor(127 * Math.sin(0.3 * this.hue + 0) + 128);
    const b = Math.floor(127 * Math.sin(0.3 * this.hue + 2) + 128);
    const c = Math.floor(127 * Math.sin(0.3 * this.hue + 4) + 128);

    this.color = `rgba(${a}, ${b}, ${c}, 0.1)`;
  }
}

// Interface for Line instance
interface LineInstance {
  angle: number[];
  color: string | null;
}

// Interface for Wave instance
interface WaveInstance {
  Waves: Waves;
  Lines: LineInstance[];
  angle: number[];
  speed: number[];
  update: () => void;
  draw: () => void;
}

// Wave class definition
class Wave implements WaveInstance {
  Waves: Waves;
  Lines: LineInstance[];
  angle: number[];
  speed: number[];

  constructor(Waves: Waves) {
    this.Waves = Waves;
    this.Lines = [];
    this.angle = [rnd(Math.PI * 2), rnd(Math.PI * 2) + Math.PI / 4, rnd(Math.PI * 2) + Math.PI / 2, rnd(Math.PI * 2) + Math.PI / 4];
    this.speed = [
      Waves.options.speed![0] * 0.1,
      Waves.options.speed![0] * 0.08,
      Waves.options.speed![0] * 0.06,
      Waves.options.speed![0] * 0.07
    ];
  }

  update() {
    this.Lines.push(new Line(this, null));
    if (this.Lines.length > this.Waves.options.width!) {
      this.Lines.shift();
    }
  }

  draw() {
    const ctx = this.Waves.ctx;
    if (!ctx) return;

    const radius = this.Waves.radius;
    const radius3 = radius / 3;
    const x = this.Waves.centerX;
    const y = this.Waves.centerY;
    const rotation = dtr(this.Waves.options.rotation!);
    const amplitude = this.Waves.options.amplitude!;
    const debug = this.Waves.options.debug!;
    const spacing = this.Waves.options.spacing || 3;

    if (debug) {
      ctx.beginPath();
      ctx.strokeStyle = "#f00";
      ctx.arc(x, y, radius, 0, Math.PI * 2);
      ctx.stroke();
    }

    this.Lines.forEach((line, i) => {
      if (debug && i > 0) return;
      if (i % spacing !== 0) return;

      const angle = line.angle;
      const offset = i * 2;

      const x1 = x - (radius - offset) * Math.cos(angle[0] * amplitude + rotation);
      const y1 = y - (radius - offset) * Math.sin(angle[0] * amplitude + rotation);
      const x2 = x + (radius - offset) * Math.cos(angle[3] * amplitude + rotation);
      const y2 = y + (radius - offset) * Math.sin(angle[3] * amplitude + rotation);
      const cpx1 = x - (radius3 - offset) * Math.cos(angle[1] * amplitude * 2);
      const cpy1 = y - (radius3 - offset) * Math.sin(angle[1] * amplitude * 2);
      const cpx2 = x + (radius3 - offset) * Math.cos(angle[2] * amplitude * 2);
      const cpy2 = y + (radius3 - offset) * Math.sin(angle[2] * amplitude * 2);

      if (debug) {
        ctx.strokeStyle = "#fff";
      } else {
        const progress = i / spacing / Math.floor(this.Lines.length / spacing);
        ctx.strokeStyle = colorManager.getInterpolatedColor(progress);
      }

      ctx.beginPath();
      ctx.moveTo(x1, y1);
      ctx.bezierCurveTo(cpx1, cpy1, cpx2, cpy2, x2, y2);
      ctx.stroke();

      if (debug) {
        ctx.strokeStyle = "#fff";
        ctx.globalAlpha = 0.3;
        ctx.beginPath();
        ctx.moveTo(x1, y1);
        ctx.lineTo(cpx1, cpy1);
        ctx.stroke();
        ctx.beginPath();
        ctx.moveTo(x2, y2);
        ctx.lineTo(cpx2, cpy2);
        ctx.stroke();
        ctx.globalAlpha = 1;
      }
    });
  }
}

class Line implements LineInstance {
  angle: number[];
  color: string | null;

  constructor(Wave: Wave, color: string | null) {
    const angle = Wave.angle;
    const speed = Wave.speed;

    this.angle = [
      Math.sin((angle[0] += speed[0])),
      Math.sin((angle[1] += speed[1])),
      Math.sin((angle[2] += speed[2])),
      Math.sin((angle[3] += speed[3]))
    ];
    this.color = color;
  }
}

function dtr(deg: number): number {
  return (deg * Math.PI) / 180;
}

function rnd(a: number, b?: number): number {
  if (typeof b === 'undefined') return Math.random() * a;
  return a + Math.random() * (b - a);
}

let colorManager: ColorManager;

const HomePage = () => {
  const [isSidebarOpen, setIsSidebarOpen] = useState(false);
  const [zoomLevel, setZoomLevel] = useState(1);
  const [panPosition, setPanPosition] = useState({ x: 0, y: 0 });
  const [isDragging, setIsDragging] = useState(false);
  const [dragStart, setDragStart] = useState({ x: 0, y: 0 });
  const [isImageHovered, setIsImageHovered] = useState(false);
  const { data: session, status } = useSession();
  const router = useRouter();

  useEffect(() => {
    console.log("HomePage: Status:", status, "Session:", !!session);
    
    // Redirect to login if not authenticated
    if (status === "unauthenticated") {
      console.log("HomePage: Redirecting unauthenticated user to login");
      router.replace("/login");
      return;
    }
  }, [status, router]);

  // Three.js Wave Animation Setup
  useEffect(() => {
    if (!session) return; // Only run when session is available

    // Color management
    colorManager = {
      startColor: "#B90A7B", // Pink matching page theme
      endColor: "#5332FF", // Purple matching page theme
      START_OPACITY: 0.75,
      END_OPACITY: 0.11,

      updateColors(newStart: string, newEnd: string) {
        this.startColor = newStart;
        this.endColor = newEnd;
      },

      getInterpolatedColor(progress: number) {
        const r1 = parseInt(this.startColor.slice(1, 3), 16);
        const g1 = parseInt(this.startColor.slice(3, 5), 16);
        const b1 = parseInt(this.startColor.slice(5, 7), 16);

        const r2 = parseInt(this.endColor.slice(1, 3), 16);
        const g2 = parseInt(this.endColor.slice(3, 5), 16);
        const b2 = parseInt(this.endColor.slice(5, 7), 16);

        const r = Math.round(r1 + (r2 - r1) * progress);
        const g = Math.round(g1 + (g2 - g1) * progress);
        const b = Math.round(b1 + (b2 - b1) * progress);

        const opacity = this.START_OPACITY + (this.END_OPACITY - this.START_OPACITY) * progress;
        return `rgba(${r}, ${g}, ${b}, ${opacity})`;
      }
    };

    // Animation configuration
    const initialParams: WaveOptions = {
      waves: 7,
      width: 359,
      rotation: 90,
      amplitude: 1.26,
      speed: [0.0085, 0.0017],
      resize: true,
      spacing: 8,
      backgroundColor: "#0a0a0a",
      startColor: "#B90A7B",
      endColor: "#5332FF",
      opacity: {
        start: 0.75,
        end: 0.11
      },
      speedMultiplier: 5
    };

    colorManager.startColor = initialParams.startColor!;
    colorManager.endColor = initialParams.endColor!;
    colorManager.START_OPACITY = initialParams.opacity!.start;
    colorManager.END_OPACITY = initialParams.opacity!.end;

    const WAVES_CONFIG = initialParams;

    // Create Waves instance
    const waves = new Waves("#holder", WAVES_CONFIG);
    waves.resize();
    waves.animate();

    // Cleanup on component unmount
    return () => {
      window.removeEventListener("resize", waves.resize);
      if (waves.holder && waves.canvas) {
        waves.holder.removeChild(waves.canvas);
      }
    };
  }, [session]);

  // Show loading state while checking authentication
  if (status === "loading") {
    return (
      <div className="min-h-screen bg-black flex items-center justify-center">
        <div className="text-center">
          <div className="w-16 h-16 rounded-full bg-gradient-to-r from-pink-500 to-purple-600 flex items-center justify-center mb-4 mx-auto">
            <div className="w-8 h-8 border-4 border-white border-t-transparent rounded-full animate-spin"></div>
          </div>
          <p className="text-gray-400">Loading...</p>
        </div>
      </div>
    );
  }

  // Show home page content when authenticated
  if (session && status === "authenticated") {
    const toggleSidebar = () => {
      setIsSidebarOpen(!isSidebarOpen);
    };

    const closeSidebar = () => {
      setIsSidebarOpen(false);
    };

    const scrollToOverview = () => {
      const overviewSection = document.getElementById('system-overview');
      if (overviewSection) {
        overviewSection.scrollIntoView({ behavior: 'smooth' });
      }
    };

    const architectureFeatures = [
      {
        icon: <Mail className="w-12 h-12" />,
        title: "Email Processing",
        description: "Advanced email analysis with smart length-based routing and topic extraction"
      },
      {
        icon: <MessageSquare className="w-12 h-12" />,
        title: "Chat Analysis",
        description: "Intelligent conversation chunking and key-phrase extraction with hallucination checks"
      },
      {
        icon: <Ticket className="w-12 h-12" />,
        title: "Ticket Classification",
        description: "Bifurcated processing for support and alert tickets with specialized LLM extraction"
      },
      {
        icon: <Phone className="w-12 h-12" />,
        title: "Voice Transcript",
        description: "Convert and analyze customer support calls with agent performance tracking and issue resolution insights"
      },
      {
        icon: <Share2 className="w-12 h-12" />,
        title: "Social Media",
        description: "Monitor and analyze social interactions with brand sentiment tracking and trend detection"
      }
    ];

    const technicalSpecs = [
      {
        title: "LLM Models",
        specs: ["Gemma 3 27b for deep contextual understanding", "Specialized extraction for different content types", "Hallucination detection and coherence scoring"]
      },
      {
        title: "Clustering Algorithms",
        specs: ["K-Means for known categories", "HDBSCAN for flexible hierarchical clustering", "Outlier detection capabilities"]
      },
      {
        title: "Integration Points",
        specs: ["Cross-pipeline key-phrase extraction", "Unified post-processing modules", "Multi-channel insight synthesis"]
      }
    ];

    

    return (
      <div className="min-h-screen relative overflow-hidden">
        <style jsx>{`
          #holder {
            width: 100vw;
            height: 100vh;
            position: fixed;
            top: 0;
            left: 0;
            z-index: 0;
            background: linear-gradient(135deg, #0a0a0a 0%, #1a0a1a 50%, #0a0a1a 100%);
          }
        `}</style>

        {/* Header */}
        <Header 
          transparent={true} 
          isLoggedIn={true} 
          isSidebarOpen={isSidebarOpen}
          onToggleSidebar={toggleSidebar}
        />

        {/* Sidebar */}
        <div className={`fixed inset-y-0 left-0 z-40 transform transition-transform duration-300 ease-in-out ${
          isSidebarOpen ? 'translate-x-0' : '-translate-x-full'
        }`}>
          <Sidebar onClose={closeSidebar} />
        </div>

        {/* Sidebar Overlay */}
        {isSidebarOpen && (
          <div 
            className="fixed inset-0 z-30"
            onClick={closeSidebar}
          />
        )}

        {/* Three.js Canvas */}
        <div id="holder" />

        {/* Gradient Overlay */}
        <div
          className="fixed inset-0 z-10 pointer-events-none"
          style={{
            background: 'linear-gradient(135deg, rgba(185, 10, 189, 0.3) 0%, rgba(83, 50, 255, 0.3) 100%)',
            mixBlendMode: 'multiply',
          }}
        />

        {/* Main Content with Blur Effect */}
        <div className={`relative z-20 pt-[72px] transition-all duration-300 ${isSidebarOpen ? 'filter blur-sm' : ''}`}>
          {/* Hero Section */}
          <section className="min-h-screen flex items-center justify-center px-4">
            <div className="text-center max-w-6xl mx-auto">
              <h1 className="text-5xl md:text-7xl font-bold mb-6 bg-gradient-to-r from-white to-gray-300 bg-clip-text text-transparent">
                Let&apos;s Explore
                <span className="block bg-gradient-to-r from-pink-400 to-purple-400 bg-clip-text text-transparent">
                  Topic Modeling
                </span>
              </h1>
              
              <p className="text-xl text-gray-200 mb-8 max-w-4xl mx-auto leading-relaxed">
                Let&apos;s explore the power of advanced language models and clustering techniques
              </p>
              
              <div className="flex flex-col sm:flex-row gap-4 justify-center items-center">
                <button 
                  onClick={toggleSidebar}
                  className="group bg-gradient-to-r from-pink-500 to-purple-600 text-white px-8 py-4 rounded-full text-lg font-semibold hover:from-pink-600 hover:to-purple-700 transition-all duration-300 flex items-center gap-2"
                >
                  Explore System
                  <ArrowRight className="w-5 h-5 group-hover:translate-x-1 transition-transform" />
                </button>
                <button 
                  onClick={scrollToOverview}
                  className="text-white border-2 border-white px-8 py-4 rounded-full text-lg font-semibold hover:bg-white hover:text-gray-900 transition-all duration-300"
                >
                  View Architecture
                </button>
              </div>
            </div>
          </section>

          {/* Main Content */}
          <div className="bg-gray-900 bg-opacity-95 backdrop-blur-sm">
            {/* Overview Section */}
            <section id="system-overview" className="py-20 px-4">
              <div className="max-w-6xl mx-auto">
                <div className="text-center mb-16">
                  <h2 className="text-4xl md:text-5xl font-bold text-white mb-6">System Overview</h2>
                  <p className="text-xl text-gray-300 max-w-4xl mx-auto leading-relaxed">
                    This topic modeling system represents an integrated approach to analyze five distinct customer 
                    communication channels (email, chat, ticket, voice, and social media data). The architecture employs various large language models 
                    (specifically Gemma models) along with advanced clustering techniques to extract 
                    meaningful topics from customer communications.
                  </p>
                </div>

                {/* System Architecture Heading */}
                <h3 className="text-2xl font-bold text-white text-center mb-2">System Architecture</h3>

                {/* System Architecture Image with Zoom Controls */}
                <div 
                  className="relative flex items-center justify-center mb-16 mt-6"
                  onMouseEnter={() => setIsImageHovered(true)}
                  onMouseLeave={() => setIsImageHovered(false)}
                >

                    {/* Image Container */}
                    <div 
                      className="overflow-hidden rounded-2xl relative"
                      style={{ 
                        width: '100%', 
                        maxHeight: '600px',
                        cursor: isDragging ? 'grabbing' : 'grab'
                      }}
                      onMouseDown={(e) => {
                        if (zoomLevel > 1) {
                          setIsDragging(true);
                          setDragStart({ x: e.clientX - panPosition.x, y: e.clientY - panPosition.y });
                        }
                      }}
                      onMouseMove={(e) => {
                        if (isDragging && zoomLevel > 1) {
                          setPanPosition({
                            x: e.clientX - dragStart.x,
                            y: e.clientY - dragStart.y
                          });
                        }
                      }}
                      onMouseUp={() => setIsDragging(false)}
                      onMouseLeave={() => setIsDragging(false)}
                    >
                      {/* Zoom Controls - Top Left Corner */}
                      <div className={`absolute -top-1 left-1 flex gap-2 z-10 transition-opacity duration-300 ${
                        isImageHovered ? 'opacity-100' : 'opacity-0'
                      }`}>
                        <button
                          onClick={() => setZoomLevel(prev => Math.min(prev + 0.5, 5))}
                          className="bg-black bg-opacity-70 text-white px-3 py-1 rounded hover:bg-opacity-90 transition-colors"
                        >
                          +
                        </button>
                        <button
                          onClick={() => setZoomLevel(prev => Math.max(prev - 0.5, 0.5))}
                          className="bg-black bg-opacity-70 text-white px-3 py-1 rounded hover:bg-opacity-90 transition-colors"
                        >
                          -
                        </button>
                        <button
                          onClick={() => {
                            setZoomLevel(1);
                            setPanPosition({ x: 0, y: 0 });
                          }}
                          className="bg-black bg-opacity-70 text-white px-3 py-1 rounded hover:bg-opacity-90 transition-colors"
                        >
                          Reset
                        </button>
                      </div>

                      {/* Zoom Level Display - Top Right Corner */}
                      <div className={`absolute -top-1 right-1 bg-black bg-opacity-70 text-white px-3 py-1 rounded z-10 transition-opacity duration-300 ${
                        isImageHovered ? 'opacity-100' : 'opacity-0'
                      }`}>
                        {Math.round(zoomLevel * 100)}%
                      </div>

                      <img 
                        src="/dia - Copy (3).svg" 
                        alt="System Architecture - Multi-channel topic modeling pipeline showing Email, Chat, and Ticket data processing with LLM operations, clustering, and post-processing workflows"
                        style={{ 
                          transform: `scale(${zoomLevel}) translate(${panPosition.x / zoomLevel}px, ${panPosition.y / zoomLevel}px)`,
                          transformOrigin: 'center',
                          width: '100%',
                          height: 'auto',
                          objectFit: 'contain'
                        }}
                        draggable={false}
                      />
                    </div>
                </div>

                {/* Architecture Features */}
                <div className="flex flex-col md:flex-row gap-2">
                  {architectureFeatures.map((feature, index) => (
                    <div key={index} className="group bg-gray-800 rounded-lg px-10 py-8 hover:bg-gray-700 transition-all duration-300 hover:scale-105 flex-1">
                      <div className="text-pink-400 mb-6 group-hover:text-purple-400 transition-colors">
                        <div className="w-12 h-12 flex items-center justify-center">
                          {feature.icon}
                        </div>
                      </div>
                      <h3 className="text-lg font-semibold text-white mb-4">{feature.title}</h3>
                      <p className="text-gray-300 text-sm">{feature.description}</p>
                    </div>
                  ))}
                </div>
              </div>
            </section>

            {/* Ticket Data Processing Pipeline */}
            <section className="py-20 px-4 bg-gradient-to-r from-gray-800 to-gray-900">
              <div className="max-w-6xl mx-auto">
                <h2 className="text-4xl font-bold text-white mb-12 text-center">Ticket Data Processing Pipeline</h2>
                
                <div className="grid md:grid-cols-2 gap-12">
                  <div className="space-y-8">
                    <div className="bg-gray-700 rounded-lg p-6 h-48 flex flex-col">
                      <h3 className="text-2xl font-semibold text-pink-400 mb-4">Initial Processing</h3>
                      <div className="space-y-3 text-gray-200 flex-1">
                        <div className="flex items-start gap-3">
                          <div className="w-2 h-2 bg-pink-400 rounded-full mt-2"></div>
                          <div>
                            <strong>Text Pre-Processor:</strong> Standardizes ticket data format, extracts structured fields, separates metadata from content, and normalizes text for consistent processing
                          </div>
                        </div>
                      </div>
                    </div>

                    <div className="bg-gray-700 rounded-lg p-6 h-48 flex flex-col">
                      <h3 className="text-2xl font-semibold text-purple-400 mb-4">Support Ticket Path</h3>
                      <div className="space-y-3 text-gray-200 flex-1">
                        <div className="flex items-start gap-3">
                          <div className="w-2 h-2 bg-purple-400 rounded-full mt-2"></div>
                          <div>
                            <strong>LLM Content Extractor:</strong> Extracts key information elements including customer issue type, requested actions, product mentions, and historical context
                          </div>
                        </div>
                      </div>
                    </div>
                  </div>

                  <div className="space-y-8">
                    <div className="bg-gray-700 rounded-lg p-6 h-48 flex flex-col">
                      <h3 className="text-2xl font-semibold text-pink-400 mb-4">Alert Ticket Path</h3>
                      <div className="space-y-3 text-gray-200 flex-1">
                        <div className="flex items-start gap-3">
                          <div className="w-2 h-2 bg-pink-400 rounded-full mt-2"></div>
                          <div>
                            <strong>LLM Key-Phrase Extractor:</strong> Specialized extraction for urgent issues, producing sub-topics and dominant topics for high-priority communications
                          </div>
                        </div>
                      </div>
                    </div>

                    <div className="bg-gray-700 rounded-lg p-6 h-48 flex flex-col">
                      <h3 className="text-2xl font-semibold text-purple-400 mb-4">K-Means Clustering</h3>
                      <div className="space-y-3 text-gray-200 flex-1">
                        <div className="flex items-start gap-3">
                          <div className="w-2 h-2 bg-purple-400 rounded-full mt-2"></div>
                          <div>
                            Organizes extracted topics into sub-topic clusters and dominant topic clusters, enabling pattern recognition across alert tickets
                          </div>
                        </div>
                      </div>
                    </div>
                  </div>
                </div>
              </div>
            </section>

            {/* Email Processing Pipeline */}
            <section className="py-20 px-4">
              <div className="max-w-6xl mx-auto">
                <h2 className="text-4xl font-bold text-white mb-12 text-center">Email Data Processing Pipeline</h2>
                
                <div className="grid lg:grid-cols-3 gap-8">
                  <div className="bg-gray-800 rounded-lg p-6">
                    <h3 className="text-xl font-semibold text-pink-400 mb-4">Initial Processing</h3>
                    <ul className="space-y-2 text-gray-200">
                      <li className="flex items-start gap-2">
                        <ChevronRight className="w-4 h-4 mt-1 text-pink-400" />
                        Header and signature removal
                      </li>
                      <li className="flex items-start gap-2">
                        <ChevronRight className="w-4 h-4 mt-1 text-pink-400" />
                        Content cleaning and formatting
                      </li>
                      <li className="flex items-start gap-2">
                        <ChevronRight className="w-4 h-4 mt-1 text-pink-400" />
                        Standardization for LLM processing
                      </li>
                    </ul>
                  </div>

                  <div className="bg-gray-800 rounded-lg p-6">
                    <h3 className="text-xl font-semibold text-purple-400 mb-4">Word Count Routing</h3>
                    <div className="space-y-4 text-gray-200">
                      <div className="p-3 bg-gray-700 rounded">
                        <strong>Emails {'>'} 700 words:</strong> Routed to Gemma 3 for summarization
                      </div>
                      <div className="p-3 bg-gray-700 rounded">
                        <strong>Emails ≤ 700 words:</strong> Direct to topic extraction
                      </div>
                    </div>
                  </div>

                  <div className="bg-gray-800 rounded-lg p-6">
                    <h3 className="text-xl font-semibold text-pink-400 mb-4">Topic Generation</h3>
                    <ul className="space-y-2 text-gray-200">
                      <li className="flex items-start gap-2">
                        <ChevronRight className="w-4 h-4 mt-1 text-pink-400" />
                        100+ dominant topics extraction
                      </li>
                      <li className="flex items-start gap-2">
                        <ChevronRight className="w-4 h-4 mt-1 text-pink-400" />
                        Hierarchical sub-topic structure
                      </li>
                      <li className="flex items-start gap-2">
                        <ChevronRight className="w-4 h-4 mt-1 text-pink-400" />
                        Email-to-topic mapping
                      </li>
                    </ul>
                  </div>
                </div>
              </div>
            </section>

            {/* Chat Processing Pipeline */}
            <section className="py-20 px-4 bg-gradient-to-r from-gray-800 to-gray-900">
              <div className="max-w-6xl mx-auto">
                <h2 className="text-4xl font-bold text-white mb-12 text-center">Chat Data Processing Pipeline</h2>
                
                <div className="grid md:grid-cols-2 gap-12">
                  <div>
                    <h3 className="text-2xl font-semibold text-pink-400 mb-6">Segmentation Phase</h3>
                    <div className="space-y-4">
                      <div className="bg-gray-700 rounded-lg p-4">
                        <h4 className="font-semibold text-white mb-2">Chunking Logic</h4>
                        <p className="text-gray-300 text-sm">
                          Creates rules for breaking conversations into meaningful units considering turn-taking, 
                          topical shifts, temporal gaps, and semantic coherence
                        </p>
                      </div>
                      <div className="bg-gray-700 rounded-lg p-4">
                        <h4 className="font-semibold text-white mb-2">LLM Chunking</h4>
                        <p className="text-gray-300 text-sm">
                          Applies chunking logic to create semantically coherent conversation segments 
                          while preserving contextual relationships
                        </p>
                      </div>
                    </div>
                  </div>

                  <div>
                    <h3 className="text-2xl font-semibold text-purple-400 mb-6">Analysis & Clustering</h3>
                    <div className="space-y-4">
                      <div className="bg-gray-700 rounded-lg p-4">
                        <h4 className="font-semibold text-white mb-2">Key-Phrase Extraction</h4>
                        <p className="text-gray-300 text-sm">
                          Extracts customer intents, pain points, product mentions, and sentiment indicators 
                          with hallucination checks for quality control
                        </p>
                      </div>
                      <div className="bg-gray-700 rounded-lg p-4">
                        <h4 className="font-semibold text-white mb-2">HDBSCAN Clustering</h4>
                        <p className="text-gray-300 text-sm">
                          Hierarchical clustering with outlier detection, creating primary and sub-clusters 
                          with automated label generation
                        </p>
                      </div>
                    </div>
                  </div>
                </div>
              </div>
            </section>

            {/* Voice Transcript Processing Pipeline */}
            <section className="py-20 px-4">
              <div className="max-w-6xl mx-auto">
                <h2 className="text-4xl font-bold text-white mb-12 text-center">Voice Transcript Processing Pipeline</h2>
                
                <div className="grid md:grid-cols-2 gap-12">
                  <div className="space-y-8">
                    <div className="bg-gray-700 rounded-lg p-6 h-48 flex flex-col">
                      <h3 className="text-2xl font-semibold text-pink-400 mb-4">Audio Processing</h3>
                      <div className="space-y-3 text-gray-200 flex-1">
                        <div className="flex items-start gap-3">
                          <div className="w-2 h-2 bg-pink-400 rounded-full mt-2"></div>
                          <div>
                            <strong>Call Transcription:</strong> High-accuracy speech-to-text conversion of customer support calls, handling multiple languages, accents, and call quality variations
                          </div>
                        </div>
                      </div>
                    </div>

                    <div className="bg-gray-700 rounded-lg p-6 h-48 flex flex-col">
                      <h3 className="text-2xl font-semibold text-purple-400 mb-4">Content Analysis</h3>
                      <div className="space-y-3 text-gray-200 flex-1">
                        <div className="flex items-start gap-3">
                          <div className="w-2 h-2 bg-purple-400 rounded-full mt-2"></div>
                          <div>
                            <strong>Customer-Agent Analysis:</strong> Analyze customer sentiment, agent performance, issue resolution patterns, and call outcome effectiveness from support conversations
                          </div>
                        </div>
                      </div>
                    </div>
                  </div>

                  <div className="space-y-8">
                    <div className="bg-gray-700 rounded-lg p-6 h-48 flex flex-col">
                      <h3 className="text-2xl font-semibold text-pink-400 mb-4">Topic Extraction</h3>
                      <div className="space-y-3 text-gray-200 flex-1">
                        <div className="flex items-start gap-3">
                          <div className="w-2 h-2 bg-pink-400 rounded-full mt-2"></div>
                          <div>
                            <strong>Support Call Topic Extraction:</strong> Extract customer issues, resolution steps, product mentions, and escalation patterns from support call transcripts
                          </div>
                        </div>
                      </div>
                    </div>

                    <div className="bg-gray-700 rounded-lg p-6 h-48 flex flex-col">
                      <h3 className="text-2xl font-semibold text-purple-400 mb-4">Quality Assurance</h3>
                      <div className="space-y-3 text-gray-200 flex-1">
                        <div className="flex items-start gap-3">
                          <div className="w-2 h-2 bg-purple-400 rounded-full mt-2"></div>
                          <div>
                            <strong>Call Quality Metrics:</strong> Validate transcription accuracy, measure call resolution success rates, and track agent performance indicators for support optimization
                          </div>
                        </div>
                      </div>
                    </div>
                  </div>
                </div>
              </div>
            </section>

            {/* Social Media Processing Pipeline */}
            <section className="py-20 px-4 bg-gradient-to-r from-gray-800 to-gray-900">
              <div className="max-w-6xl mx-auto">
                <h2 className="text-4xl font-bold text-white mb-12 text-center">Social Media Processing Pipeline</h2>
                
                <div className="grid md:grid-cols-2 gap-12">
                  <div>
                    <h3 className="text-2xl font-semibold text-pink-400 mb-6">Data Collection & Filtering</h3>
                    <div className="space-y-4">
                      <div className="bg-gray-700 rounded-lg p-4">
                        <h4 className="font-semibold text-white mb-2">Multi-Platform Aggregation</h4>
                        <p className="text-gray-300 text-sm">
                          Collect posts, comments, mentions, and reviews from Twitter, Trustpilot, Reddit, 
                          Google Play Store, and App Store with real-time monitoring capabilities
                        </p>
                      </div>
                      <div className="bg-gray-700 rounded-lg p-4">
                        <h4 className="font-semibold text-white mb-2">Content Preprocessing</h4>
                        <p className="text-gray-300 text-sm">
                          Clean and standardize content from Twitter (tweets, replies), Trustpilot (reviews), 
                          Reddit (posts, comments), and app stores (reviews, ratings) for consistent analysis
                        </p>
                      </div>
                    </div>
                  </div>

                  <div>
                    <h3 className="text-2xl font-semibold text-purple-400 mb-6">Sentiment & Trend Analysis</h3>
                    <div className="space-y-4">
                      <div className="bg-gray-700 rounded-lg p-4">
                        <h4 className="font-semibold text-white mb-2">Brand Sentiment Tracking</h4>
                        <p className="text-gray-300 text-sm">
                          Analyze brand mentions across Twitter, Trustpilot reviews, Reddit discussions, 
                          and app store feedback with real-time sentiment scoring and trend identification
                        </p>
                      </div>
                      <div className="bg-gray-700 rounded-lg p-4">
                        <h4 className="font-semibold text-white mb-2">Review & Discussion Analysis</h4>
                        <p className="text-gray-300 text-sm">
                          Track review trends, identify key discussion topics on Reddit, monitor 
                          Twitter engagement, and analyze app store rating patterns for strategic insights
                        </p>
                      </div>
                    </div>
                  </div>
                </div>
              </div>
            </section>

            {/* Technical Implementation */}
            <section className="py-20 px-4">
              <div className="max-w-6xl mx-auto">
                <h2 className="text-4xl font-bold text-white mb-12 text-center">Technical Implementation</h2>
                
                <div className="grid md:grid-cols-3 gap-8">
                  {technicalSpecs.map((spec, index) => (
                    <div key={index} className="bg-gray-800 rounded-lg p-6">
                      <h3 className="text-xl font-semibold text-pink-400 mb-4">{spec.title}</h3>
                      <ul className="space-y-2">
                        {spec.specs.map((item, itemIndex) => (
                          <li key={itemIndex} className="flex items-start gap-2 text-gray-300">
                            <div className="w-2 h-2 bg-purple-400 rounded-full mt-2"></div>
                            <span className="text-sm">{item}</span>
                          </li>
                        ))}
                      </ul>
                    </div>
                  ))}
                </div>
              </div>
            </section>

            

                         {/* Integration & Verification */}
             <section className="py-20 px-4 bg-gradient-to-r from-gray-800 to-gray-900">
              <div className="max-w-6xl mx-auto">
                <div className="grid md:grid-cols-2 gap-12 items-center">
                  <div>
                    <h2 className="text-4xl font-bold text-white mb-6">
                      Integration & Human Verification
                    </h2>
                    <p className="text-lg text-gray-300 mb-6">
                      All data streams converge through dedicated post-processing modules that perform 
                      standardization, cross-referencing, and synthesis of insights from multiple channels.
                    </p>
                    <div className="space-y-4 mb-8">
                      <div className="flex items-center gap-3">
                        <div className="w-2 h-2 bg-pink-400 rounded-full"></div>
                        <span className="text-gray-200">Cross-referencing between communication channels</span>
                      </div>
                      <div className="flex items-center gap-3">
                        <div className="w-2 h-2 bg-purple-400 rounded-full"></div>
                        <span className="text-gray-200">Priority scoring and trend analysis</span>
                      </div>
                      <div className="flex items-center gap-3">
                        <div className="w-2 h-2 bg-pink-400 rounded-full"></div>
                        <span className="text-gray-200">Manual analysis and verification workflow</span>
                      </div>
                    </div>
                  </div>
                  
                  <div className="bg-gray-800 rounded-lg p-8">
                    <h3 className="text-xl font-semibold text-white mb-6">Human-in-the-Loop Process</h3>
                    <div className="space-y-4">
                      <div className="flex items-start gap-3">
                        <Shield className="w-5 h-5 text-pink-400 mt-1" />
                        <div>
                          <h4 className="text-white font-medium">Quality Review</h4>
                          <p className="text-gray-400 text-sm">Automated categorization accuracy verification</p>
                        </div>
                      </div>
                      <div className="flex items-start gap-3">
                        <TrendingUp className="w-5 h-5 text-purple-400 mt-1" />
                        <div>
                          <h4 className="text-white font-medium">Trend Analysis</h4>
                          <p className="text-gray-400 text-sm">Emerging issue identification and categorization</p>
                        </div>
                      </div>
                      <div className="flex items-start gap-3">
                        <Users className="w-5 h-5 text-pink-400 mt-1" />
                        <div>
                          <h4 className="text-white font-medium">Feedback Loop</h4>
                          <p className="text-gray-400 text-sm">Continuous system improvement through human input</p>
                        </div>
                      </div>
                    </div>
                  </div>
                </div>
              </div>
            </section>      
          </div>
        </div>
      </div>
    );
  }

  return (
    <AuthGuard requireAuth={true}>
      <div className="min-h-screen bg-black flex items-center justify-center">
        <div className="text-center">
          <div className="w-16 h-16 rounded-full bg-gradient-to-r from-pink-500 to-purple-600 flex items-center justify-center mb-4 mx-auto">
            <div className="w-8 h-8 border-4 border-white border-t-transparent rounded-full animate-spin"></div>
          </div>
          <p className="text-gray-400">Redirecting...</p>
        </div>
      </div>
    </AuthGuard>
  );
};

export default HomePage;