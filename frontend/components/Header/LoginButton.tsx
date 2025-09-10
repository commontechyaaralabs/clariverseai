import Link from 'next/link';
import React from 'react';

interface LoginButtonProps {
  className?: string;
  href?: string;
  onLoginClick?: () => void;
}

const LoginButton: React.FC<LoginButtonProps> = ({ 
  className = "",
  href = "/login",
  onLoginClick
}) => {
  return (
    <>
      {onLoginClick ? (
        <button 
          onClick={onLoginClick}
          className={`bg-gradient-to-r from-pink-500 to-purple-600 text-white px-6 py-2 rounded-full hover:from-pink-600 hover:to-purple-700 transition-all duration-300 transform hover:scale-105 font-medium ${className}`}
        >
          Login
        </button>
      ) : (
        <Link href={href}>
          <button className={`bg-gradient-to-r from-pink-500 to-purple-600 text-white px-6 py-2 rounded-full hover:from-pink-600 hover:to-purple-700 transition-all duration-300 transform hover:scale-105 font-medium ${className}`}>
            Login
          </button>
        </Link>
      )}
    </>
  );
};

export { LoginButton };