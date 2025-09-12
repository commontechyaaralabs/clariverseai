"use client";

import React, { useEffect, useRef } from 'react';
import { Brain, Mail, MessageSquare, Ticket, TrendingUp, ArrowRight, Database, Settings, Shield, Zap, Phone, Share2 } from 'lucide-react';
import {Header} from '@/components/Header/Header';
import * as THREE from 'three';
import './globals.css';
import { useRouter } from 'next/navigation';
import { useSession } from 'next-auth/react';

declare global {
  interface Window {
    THREE: typeof THREE;
  }
}


const HomePage: React.FC = () => {
  const router = useRouter();
  const { data: session, status } = useSession();
  const containerRef = useRef<HTMLDivElement>(null);
  const sceneRef = useRef<THREE.Scene | null>(null);
  const rendererRef = useRef<THREE.WebGLRenderer | null>(null);
  const animationFrameRef = useRef<number>(0);

  const initShaderBackground = () => {
    if (!containerRef.current || !window.THREE) return;

    const container = containerRef.current;
    const scene = new window.THREE.Scene();
    const clock = new window.THREE.Clock();
    
    const camera = new window.THREE.OrthographicCamera( 
      window.innerWidth / -2, 
      window.innerWidth / 2, 
      window.innerHeight / 2, 
      window.innerHeight / -2, 
      -5000, 
      5000 
    );
    camera.position.set(30, 30, 30);
    camera.updateProjectionMatrix();
    camera.lookAt(scene.position);

    const cubeSize = 80;
    const geometry = new window.THREE.BoxGeometry(1, cubeSize * 4, 1);
    
    const uniforms = {
      time: { value: 1.0 }
    };

    const fragmentShader = `
      uniform float time;
      varying vec2 vUv;
      void main( void ) {
        vec2 position = - 0.0 + 3.0 * vUv;
        float wave1 = abs( sin( position.x * position.y + time / 8.0 ) );
        float wave2 = abs( sin( position.x * position.y + time / 6.0 ) );
        float wave3 = abs( sin( position.x * position.y + time / 4.0 ) );
        
        float red = mix(0.7, 1.0, wave1) * wave2;
        float green = mix(0.2, 0.4, wave2) * wave3;
        float blue = mix(0.7, 0.9, wave3) * wave1;
        
        gl_FragColor = vec4( red, green, blue, 0.8 );
      }
    `;

    const vertexShader = `
      varying vec2 vUv;
      void main() {
        vUv = uv;
        vec4 mvPosition = modelViewMatrix * vec4( position, 1.0 );
        gl_Position = projectionMatrix * mvPosition;
      }
    `;

    const material = new window.THREE.ShaderMaterial({
      uniforms: uniforms,
      vertexShader: vertexShader,
      fragmentShader: fragmentShader,
      transparent: true
    });

    const meshes: THREE.Mesh[] = [];
    for (let i = 0; i < 2000; i++) {
      const mesh = new window.THREE.Mesh(geometry, material);
      mesh.position.z = i * 4 - cubeSize * 50;
      mesh.rotation.z = i * 0.01;
      scene.add(mesh);
      meshes.push(mesh);
    }

    const renderer = new window.THREE.WebGLRenderer({ antialias: true, alpha: true });
    renderer.setPixelRatio(window.devicePixelRatio);
    renderer.setClearColor(0x000000, 1);
    renderer.setSize(window.innerWidth, window.innerHeight);
    
    container.appendChild(renderer.domElement);
    
    sceneRef.current = scene;
    rendererRef.current = renderer;

    const animate = () => {
      animationFrameRef.current = requestAnimationFrame(animate);
      
      const delta = clock.getDelta();
      uniforms.time.value += delta * 2;
      
      camera.rotation.x += delta * 0.05;
      camera.rotation.z += delta * 0.05;
      
      meshes.forEach((object, i) => {
        object.rotation.x += 0.02;
        object.rotation.z += 0.02;
        object.rotation.y += delta * 0.4 * (i % 2 ? 1 : -1);
      });
      
      renderer.render(scene, camera);
    };

    animate();

    const handleResize = () => {
      camera.left = window.innerWidth / -2;
      camera.right = window.innerWidth / 2;
      camera.top = window.innerHeight / 2;
      camera.bottom = window.innerHeight / -2;
      camera.updateProjectionMatrix();
      renderer.setSize(window.innerWidth, window.innerHeight);
    };

    window.addEventListener('resize', handleResize);
    
    return () => {
      window.removeEventListener('resize', handleResize);
    };
  };

  // Check authentication status and redirect if needed
  useEffect(() => {
    console.log("LandingPage: Status:", status, "Session:", !!session);
    
    if (status === "loading") return;
    
    // If user is authenticated, redirect to home page
    if (session) {
      console.log("LandingPage: Redirecting authenticated user to /home");
      router.replace('/home');
      return;
    }
  }, [session, status, router]);

  // Initialize Three.js background for non-authenticated users
  useEffect(() => {
    if (session) return; // Don't run if user is authenticated

    const script = document.createElement('script');
    script.src = 'https://cdnjs.cloudflare.com/ajax/libs/three.js/r128/three.min.js';
    script.onload = () => {
      initShaderBackground();
    };
    document.head.appendChild(script);

    return () => {
      if (animationFrameRef.current) {
        cancelAnimationFrame(animationFrameRef.current);
      }
      if (rendererRef.current) {
        rendererRef.current.dispose();
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

  // If user is authenticated, don't render the landing page
  if (session) {
    return null;
  }

  const scrollToFeatures = () => {
    const featuresSection = document.getElementById('product-features');
    if (featuresSection) {
      featuresSection.scrollIntoView({ behavior: 'smooth' });
    }
  };

  const handleLoginClick = () => {
    router.push('/login');
  };

  const productFeatures = [
    {
      icon: <Mail className="w-12 h-12" />,
      title: "Email Analysis",
      description: "Automatically categorize and prioritize emails to streamline customer support"
    },
    {
      icon: <MessageSquare className="w-12 h-12" />,
      title: "Chat Insights",
      description: "Extract key insights from chat conversations to enhance customer engagement"
    },
    {
      icon: <Ticket className="w-12 h-12" />,
      title: "Ticket Management",
      description: "Smart ticket classification for faster resolution and better resource allocation"
    },
    {
      icon: <Phone className="w-12 h-12" />,
      title: "Voice Transcript",
      description: "Convert and analyze voice conversations to understand customer sentiment and needs"
    },
    {
      icon: <Share2 className="w-12 h-12" />,
      title: "Social Media",
      description: "Monitor and analyze social media interactions to track brand sentiment and trends"
    }
  ];

  const businessBenefits = [
    { icon: <Database className="w-6 h-6" />, title: "Cross-Channel Intelligence", description: "Unified analysis across email, chat, ticket, voice, and social media data" },
    { icon: <Settings className="w-6 h-6" />, title: "Automated Categorization", description: "Efficient routing and prioritization of customer issues" },
    { icon: <TrendingUp className="w-6 h-6" />, title: "Trend Identification", description: "Early detection of emerging problems and opportunities" },
    { icon: <Brain className="w-6 h-6" />, title: "Knowledge Discovery", description: "Surface insights missed in manual review processes" },
    { icon: <Shield className="w-6 h-6" />, title: "Quality Assurance", description: "Human verification ensures reliable outputs" },
    { icon: <Zap className="w-6 h-6" />, title: "Scalable Processing", description: "Handle growing volumes of customer communications" }
  ];

  return (
    <div className="min-h-screen relative" style={{ backgroundColor: '#010101' }}>
      {/* Three.js Shader Background */}
      <div className="banner fixed inset-0 flex flex-col items-center justify-center text-center z-0 overflow-hidden" style={{ minHeight: '100vh' }}>
        <div 
          ref={containerRef}
          className="absolute inset-0 w-full h-full"
          style={{ minHeight: '100vh' }}
        />
        
        <div
          className="absolute inset-0 z-10 pointer-events-none"
          style={{
            background: 'linear-gradient(135deg, rgba(185, 10, 189, 0.3) 0%, rgba(83, 50, 255, 0.3) 100%)',
            mixBlendMode: 'multiply',
          }}
        />
        
        <div
          className="absolute inset-0 z-20 pointer-events-none"
          style={{
            background: 'rgba(255, 255, 255, 0.05)',
            animation: 'fadeIn 10s ease-in-out infinite alternate',
          }}
        />
      </div>

      {/* Navigation Bar - Public version without sidebar */}
      <Header transparent={true} isLoggedIn={false} onLoginClick={handleLoginClick} />

      {/* Hero Section */}
      <section className="relative z-30 min-h-screen flex items-center justify-center px-4">
        <div className="text-center max-w-6xl mx-auto transition-all duration-1000 opacity-100 translate-y-0">
          <h1 className="text-5xl md:text-7xl font-bold mb-6 bg-gradient-to-r from-white to-gray-300 bg-clip-text text-transparent">
            Transform Your Business
            <span className="block bg-gradient-to-r from-pink-400 to-purple-400 bg-clip-text text-transparent">
              with AI-Powered Insights
            </span>
          </h1>
          
          <p className="text-xl md:text-2xl text-gray-200 mb-8 max-w-4xl mx-auto leading-relaxed">
            Clariverse&apos; Topic Modeling Platform empowers businesses to unlock actionable insights from customer communications, driving efficiency and growth
          </p>
          
          <div className="flex flex-col sm:flex-row gap-4 justify-center items-center">
            <button 
              onClick={() => router.push('/login')}
              className="group bg-gradient-to-r from-pink-500 to-purple-600 text-white px-8 py-4 rounded-full text-lg font-semibold hover:from-pink-600 hover:to-purple-700 transition-all duration-300 flex items-center gap-2"
            >
              Request a Demo
              <ArrowRight className="w-5 h-5 group-hover:translate-x-1 transition-transform" />
            </button>
            <button 
              onClick={scrollToFeatures}
              className="text-white border-2 border-white px-8 py-4 rounded-full text-lg font-semibold hover:bg-white hover:text-gray-900 transition-all duration-300"
            >
              Discover Features
            </button>
          </div>
        </div>
      </section>

      {/* Main Content */}
      <div className="relative z-30 bg-gray-900 bg-opacity-95 backdrop-blur-sm">
        {/* Product Features */}
        <section id="product-features" className="py-20 px-4">
          <div className="max-w-6xl mx-auto">
            <div className="text-center mb-16">
              <h2 className="text-4xl md:text-5xl font-bold text-white mb-6">Why Use Our Platform?</h2>
              <p className="text-xl text-gray-300 max-w-4xl mx-auto leading-relaxed">
                Our Topic Modeling Platform leverages advanced AI to analyze customer communications, providing businesses with the tools to make data-driven decisions
              </p>
            </div>

            <div className="grid md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 2xl:grid-cols-5 gap-6">
              {productFeatures.map((feature, index) => (
                <div key={index} className="group bg-gray-800 rounded-xl p-6 hover:bg-gray-700 transition-all duration-300 hover:scale-105 min-h-[240px] flex flex-col">
                  <div className="text-pink-400 mb-4 group-hover:text-purple-400 transition-colors">
                    <div className="w-12 h-12 flex items-center justify-center">
                      {feature.icon}
                    </div>
                  </div>
                  <h3 className="text-xl font-semibold text-white mb-3">{feature.title}</h3>
                  <p className="text-gray-300 leading-relaxed flex-grow text-sm">{feature.description}</p>
                </div>
              ))}
            </div>
          </div>
        </section>

        {/* Business Value Propositions */}
        <section className="py-20 px-4 bg-gradient-to-r from-gray-800 to-gray-900">
          <div className="max-w-6xl mx-auto">
            <div className="text-center mb-16">
              <h2 className="text-4xl md:text-5xl font-bold text-white mb-6">Business Value Propositions</h2>
              <p className="text-xl text-gray-300 max-w-3xl mx-auto">
                This system delivers comprehensive business benefits through sophisticated automated analysis 
                with human oversight for quality control and actionable insights
              </p>
            </div>

            <div className="grid md:grid-cols-2 lg:grid-cols-3 gap-8">
              {businessBenefits.map((benefit, index) => (
                <div key={index} className="bg-gray-700 rounded-lg p-6 hover:bg-gray-600 transition-all duration-300">
                  <div className="text-pink-400 mb-4">
                    {benefit.icon}
                  </div>
                  <h3 className="text-lg font-semibold text-white mb-2">{benefit.title}</h3>
                  <p className="text-gray-300 text-sm">{benefit.description}</p>
                </div>
              ))}
            </div>
          </div>
        </section>

        {/* CTA Section */}
        <section id="contact" className="py-20 px-4 bg-gradient-to-r from-pink-500 to-purple-600">
          <div className="max-w-4xl mx-auto text-center">
            <h2 className="text-4xl md:text-5xl font-bold text-white mb-6">
              Start Your AI Journey Today
            </h2>
            <p className="text-xl text-pink-100 mb-8 max-w-2xl mx-auto">
              See how Clariverse can transform your customer communications and drive business success
            </p>
            <div className="flex flex-col sm:flex-row gap-4 justify-center">
              <button className="bg-white text-purple-600 px-8 py-4 rounded-full text-lg font-semibold hover:bg-gray-100 transition-all duration-300">
                Schedule a Consultation
              </button>
              <button className="border-2 border-white text-white px-8 py-4 rounded-full text-lg font-semibold hover:bg-white hover:text-purple-600 transition-all duration-300">
                Explore Case Studies
              </button>
            </div>
          </div>
        </section>

        {/* Footer */}
        <footer className="py-12 px-4 bg-gray-900 border-t border-gray-800">
          <div className="max-w-6xl mx-auto">
            <div className="grid md:grid-cols-2 gap-12 items-start">
              <div className="space-y-4">
                <div className="flex items-center">
                  <div className="w-8 h-8 rounded-full bg-gradient-to-r from-pink-500 to-purple-600 flex items-center justify-center">
                    <span className="text-white font-bold">C</span>
                  </div>
                  <span className="text-white text-lg ml-2 font-semibold">Clariverse</span>
                </div>
                <p className="text-gray-400 max-w-md">Empowering businesses with AI-driven insights</p>
              </div>
              <div className="space-y-4">
                <h4 className="text-white font-semibold text-lg">Contact</h4>
                <div className="space-y-3 text-gray-400">
                  <div>contact@clariverse.com</div>
                  <div>+1 (555) 123-4567</div>
                  <div>San Francisco, CA</div>
                </div>
              </div>
            </div>
                         <div className="border-t border-gray-800 mt-8 pt-8 text-center text-gray-400">
               <p>© {new Date().getFullYear()} Clariverse. All rights reserved.</p>
             </div>
          </div>
        </footer>
      </div>
    </div>
  );
};

export default HomePage;