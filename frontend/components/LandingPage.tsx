import React from 'react';
import { ChevronRight, Brain, Zap, Target, Users, ArrowRight, Quote } from 'lucide-react';
import {Header} from '@/components/Header/Header';
import Link from 'next/link';
import ThreeJSBackground from './ThreeJSBackground';

export function LandingPage() {
  const services = [
    {
      icon: <Brain className="w-8 h-8" />,
      title: "AI Strategy & Consulting",
      description: "Strategic guidance to integrate AI solutions that drive measurable business impact"
    },
    {
      icon: <Zap className="w-8 h-8" />,
      title: "Custom AI Development",
      description: "Tailored AI solutions built specifically for your unique business challenges"
    },
      {
      icon: <Target className="w-8 h-8" />,
      title: "Topic Modeling & NLP",
      description: "Advanced natural language processing for business communications analysis"
    },
    {
      icon: <Users className="w-8 h-8" />,
      title: "AI Implementation",
      description: "End-to-end deployment and integration of AI systems into your workflow"
    }
  ];

  const stats = [
    { number: "500+", label: "AI Solutions Deployed" },
    { number: "98%", label: "Client Satisfaction" },
    { number: "15+", label: "Industries Served" },
    { number: "50M+", label: "Data Points Analyzed" }
  ];

  const testimonials = [
    {
      quote: "Clariverse transformed our customer support with their topic modeling solution. We now process 10x more communications with better insights.",
      author: "Sarah Chen",
      role: "CTO, TechCorp"
    },
    {
      quote: "Their AI strategy helped us identify opportunities we never knew existed. ROI was evident within the first quarter.",
      author: "Michael Rodriguez",
      role: "VP Operations, GlobalTech"
    }
  ];

  return (
    <div className="min-h-screen relative" style={{ backgroundColor: '#010101' }}>
      {/* Three.js Background */}
      <ThreeJSBackground />

      {/* Navigation Bar */}
      <Header />

      {/* Hero Section */}
      <section className="relative z-20 min-h-screen flex items-center justify-center px-4">
        <div className="text-center max-w-6xl mx-auto transition-all duration-1000 opacity-100 translate-y-0">
          <h1 className="text-5xl md:text-7xl font-bold mb-6 bg-gradient-to-r from-white to-gray-300 bg-clip-text text-transparent">
            AI Solutions That Drive
            <span className="block bg-gradient-to-r from-pink-400 to-purple-400 bg-clip-text text-transparent">
              Real Business Impact
            </span>
          </h1>
          
          <p className="text-xl md:text-2xl text-gray-200 mb-8 max-w-4xl mx-auto leading-relaxed">
            From strategy to implementation, we transform your business communications with advanced AI-powered topic modeling and intelligent automation
          </p>
          
          <div className="flex flex-col sm:flex-row gap-4 justify-center items-center">
            <Link href="/login">
              <button className="group bg-gradient-to-r from-pink-500 to-purple-600 text-white px-8 py-4 rounded-full text-lg font-semibold hover:from-pink-600 hover:to-purple-700 transition-all duration-300 flex items-center gap-2">
                Get Started
                <ArrowRight className="w-5 h-5 group-hover:translate-x-1 transition-transform" />
              </button>
            </Link>
            <button className="text-white border-2 border-white px-8 py-4 rounded-full text-lg font-semibold hover:bg-white hover:text-gray-900 transition-all duration-300">
              View Our Work
            </button>
          </div>
        </div>
      </section>

      {/* Main Content */}
      <div className="relative z-30 bg-gray-900 bg-opacity-95 backdrop-blur-sm">
        
        {/* Stats Section */}
        <section className="py-16 px-4">
          <div className="max-w-6xl mx-auto">
            <div className="grid grid-cols-2 md:grid-cols-4 gap-8">
              {stats.map((stat, index) => (
                <div key={index} className="text-center">
                  <div className="text-3xl md:text-4xl font-bold text-pink-400 mb-2">{stat.number}</div>
                  <div className="text-gray-300">{stat.label}</div>
                </div>
              ))}
            </div>
          </div>
        </section>

        {/* Services Section */}
        <section id="solutions" className="py-20 px-4">
          <div className="max-w-6xl mx-auto">
            <div className="text-center mb-16">
              <h2 className="text-4xl md:text-5xl font-bold text-white mb-4">Our Solutions</h2>
              <p className="text-xl text-gray-300 max-w-3xl mx-auto">
                Comprehensive AI solutions designed to transform your business operations and customer communications
              </p>
            </div>

            <div className="grid md:grid-cols-2 lg:grid-cols-4 gap-8">
              {services.map((service, index) => (
                <div key={index} className="group bg-gray-800 rounded-lg p-6 hover:bg-gray-700 transition-all duration-300 hover:scale-105">
                  <div className="text-pink-400 mb-4 group-hover:text-purple-400 transition-colors">
                    {service.icon}
                  </div>
                  <h3 className="text-xl font-semibold text-white mb-3">{service.title}</h3>
                  <p className="text-gray-300">{service.description}</p>
                </div>
              ))}
            </div>
          </div>
        </section>

        {/* Featured Solution */}
        <section className="py-20 px-4 bg-gradient-to-r from-gray-800 to-gray-900">
          <div className="max-w-6xl mx-auto">
            <div className="grid md:grid-cols-2 gap-12 items-center">
              <div>
                <h2 className="text-4xl font-bold text-white mb-6">
                  Topic Modeling for Business Communications
                </h2>
                <p className="text-lg text-gray-300 mb-6">
                  Our advanced topic modeling system analyzes customer communications across email, chat, and ticket data using state-of-the-art language models and clustering techniques.
                </p>
                <div className="space-y-4 mb-8">
                  <div className="flex items-center gap-3">
                    <div className="w-2 h-2 bg-pink-400 rounded-full"></div>
                    <span className="text-gray-200">Cross-channel intelligence and unified analysis</span>
                  </div>
                  <div className="flex items-center gap-3">
                    <div className="w-2 h-2 bg-purple-400 rounded-full"></div>
                    <span className="text-gray-200">Automated categorization and trend identification</span>
                  </div>
                  <div className="flex items-center gap-3">
                    <div className="w-2 h-2 bg-pink-400 rounded-full"></div>
                    <span className="text-gray-200">Human-in-the-loop verification for quality assurance</span>
                  </div>
                </div>
                <button className="bg-gradient-to-r from-pink-500 to-purple-600 text-white px-6 py-3 rounded-lg hover:from-pink-600 hover:to-purple-700 transition-all duration-300 flex items-center gap-2">
                  Learn More
                  <ChevronRight className="w-4 h-4" />
                </button>
              </div>
              <div className="relative">
                <div className="bg-gray-700 rounded-lg p-8 relative overflow-hidden">
                  <div className="absolute top-0 right-0 w-20 h-20 bg-gradient-to-br from-pink-500 to-purple-600 opacity-20 rounded-full transform translate-x-6 -translate-y-6"></div>
                  <div className="relative z-10">
                    <div className="flex items-center gap-2 mb-4">
                      <Brain className="w-6 h-6 text-pink-400" />
                      <span className="text-white font-semibold">AI-Powered Analysis</span>
                    </div>
                    <div className="space-y-3">
                      <div className="flex items-center justify-between">
                        <span className="text-gray-300">Email Processing</span>
                        <span className="text-green-400">Active</span>
                      </div>
                      <div className="flex items-center justify-between">
                        <span className="text-gray-300">Chat Analysis</span>
                        <span className="text-green-400">Active</span>
                      </div>
                      <div className="flex items-center justify-between">
                        <span className="text-gray-300">Ticket Classification</span>
                        <span className="text-green-400">Active</span>
                      </div>
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </section>

        {/* Testimonials */}
        <section className="py-20 px-4">
          <div className="max-w-6xl mx-auto">
            <div className="text-center mb-16">
              <h2 className="text-4xl font-bold text-white mb-4">What Our Clients Say</h2>
              <p className="text-xl text-gray-300">Trusted by leading companies worldwide</p>
            </div>

            <div className="grid md:grid-cols-2 gap-8">
              {testimonials.map((testimonial, index) => (
                <div key={index} className="bg-gray-800 rounded-lg p-8 relative">
                  <Quote className="w-8 h-8 text-pink-400 mb-4" />
                  <p className="text-gray-200 text-lg mb-6 italic">&quot;{testimonial.quote}&quot;</p>
                  <div className="flex items-center gap-4">
                    <div className="w-12 h-12 bg-gradient-to-r from-pink-500 to-purple-600 rounded-full flex items-center justify-center">
                      <span className="text-white font-semibold">{testimonial.author.charAt(0)}</span>
                    </div>
                    <div>
                      <div className="text-white font-semibold">{testimonial.author}</div>
                      <div className="text-gray-400">{testimonial.role}</div>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          </div>
        </section>

        {/* CTA Section */}
        <section id="contact" className="py-20 px-4 bg-gradient-to-r from-pink-500 to-purple-600">
          <div className="max-w-4xl mx-auto text-center">
            <h2 className="text-4xl md:text-5xl font-bold text-white mb-6">
              Ready to Transform Your Business?
            </h2>
            <p className="text-xl text-pink-100 mb-8 max-w-2xl mx-auto">
              Let&apos;s discuss how our AI solutions can drive real impact for your organization
            </p>
            <div className="flex flex-col sm:flex-row gap-4 justify-center">
              <button className="bg-white text-purple-600 px-8 py-4 rounded-full text-lg font-semibold hover:bg-gray-100 transition-all duration-300">
                Schedule a Consultation
              </button>
              <button className="border-2 border-white text-white px-8 py-4 rounded-full text-lg font-semibold hover:bg-white hover:text-purple-600 transition-all duration-300">
                View Case Studies
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
                <p className="text-gray-400 max-w-md">AI-driven solutions for tomorrow&apos;s challenges</p>
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
              <p>&copy; {new Date().getFullYear()} Clariverse. All rights reserved.</p>
            </div>
          </div>
        </footer>
      </div>
    </div>
  );
} 