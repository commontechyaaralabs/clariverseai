import React from 'react';
import { Brain } from 'lucide-react';

interface LogoProps {
  className?: string;
}

const Logo: React.FC<LogoProps> = ({ className = "" }) => {
  return (
    <div className={`flex items-center ${className}`}>
      <div className="w-10 h-10 rounded-full bg-gradient-to-r from-pink-500 to-purple-600 flex items-center justify-center">
        <Brain className="w-5 h-5 text-white" />
      </div>
      <span className="text-white text-xl ml-2 font-semibold">Clariverse</span>
    </div>
  );
};

export { Logo };