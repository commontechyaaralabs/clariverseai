"use client";

import { useState, useEffect } from "react";
import { useRouter } from "next/navigation";
import { Eye, EyeOff, Mail, Lock, User, ArrowRight, Check, X } from "lucide-react";
import { signupAction } from "./actions";
import { useAuthHistory } from "@/hooks/useAuthHistory";

export function SignupForm() {
  const router = useRouter();
  const { navigateToLogin } = useAuthHistory();
  const [error, setError] = useState("");
  const [showPassword, setShowPassword] = useState(false);
  const [showConfirmPassword, setShowConfirmPassword] = useState(false);
  const [isLoading, setIsLoading] = useState(false);

  // Validation states
  const [emailValid, setEmailValid] = useState(false);
  const [passwordValid, setPasswordValid] = useState({
    length: false,
    number: false,
    special: false,
    uppercase: false
  });
  const [passwordsMatch, setPasswordsMatch] = useState(false);

  // Form state
  const [formData, setFormData] = useState({
    fullName: "",
    email: "",
    password: "",
    confirmPassword: ""
  });

  // Email validation
  useEffect(() => {
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    setEmailValid(emailRegex.test(formData.email));
  }, [formData.email]);

  // Password validation
  useEffect(() => {
    setPasswordValid({
      length: formData.password.length >= 8,
      number: /\d/.test(formData.password),
      special: /[!@#$%^&*(),.?":{}|<>]/.test(formData.password),
      uppercase: /[A-Z]/.test(formData.password)
    });
  }, [formData.password]);

  // Confirm password validation
  useEffect(() => {
    setPasswordsMatch(formData.password === formData.confirmPassword && formData.password.length > 0);
  }, [formData.password, formData.confirmPassword]);

  const isFormValid = () => {
    return (
      formData.fullName.trim().length >= 2 &&
      emailValid &&
      passwordValid.length &&
      passwordValid.number &&
      passwordValid.special &&
      passwordValid.uppercase &&
      passwordsMatch
    );
  };

  const handleInputChange = (field: string, value: string) => {
    setFormData(prev => ({ ...prev, [field]: value }));
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError("");

    if (!formData.fullName || !formData.email || !formData.password || !formData.confirmPassword) {
      setError("All fields are required!");
      return;
    }

    if (!isFormValid()) {
      setError("Please fix all validation errors before submitting.");
      return;
    }

    setIsLoading(true);

    try {
      const result = await signupAction(formData);
      
      if (result.success) {
        // Use the custom navigation to ensure proper history
        navigateToLogin("Registration successful! Please sign in.");
      } else {
        setError(result.error || "Registration failed. Please try again.");
      }
    } catch (error) {
      console.error("Registration error:", error);
      setError("Error connecting to the server. Please check your internet connection.");
    }

    setIsLoading(false);
  };

  const handleKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === "Enter" && !isLoading && isFormValid()) {
      handleSubmit(e);
    }
  };

  return (
    <>
      {error && (
        <div className="mb-6 p-4 bg-red-900/20 border border-red-600 rounded-lg">
          <p className="text-red-300 text-sm">{error}</p>
        </div>
      )}

      <form onSubmit={handleSubmit} className="space-y-6">
        {/* Full Name */}
        <div>
          <label className="block text-sm font-medium text-gray-300 mb-2">
            Full Name
          </label>
          <div className="relative">
            <div className="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none">
              <User className="h-5 w-5 text-gray-400" />
            </div>
            <input
              type="text"
              value={formData.fullName}
              onChange={(e) => handleInputChange("fullName", e.target.value)}
              onKeyDown={handleKeyDown}
              className={`w-full pl-10 pr-4 py-3 bg-gray-800 border rounded-lg text-white placeholder-gray-400 focus:outline-none focus:ring-2 focus:border-transparent transition-all duration-300 hover:bg-gray-750 ${
                formData.fullName.trim().length >= 2 
                  ? 'border-green-500 focus:ring-green-500' 
                  : formData.fullName.length > 0 
                  ? 'border-red-500 focus:ring-red-500' 
                  : 'border-gray-700 focus:ring-pink-500'
              }`}
              placeholder="Enter your full name"
              required
            />
          </div>
          {formData.fullName.length > 0 && formData.fullName.trim().length < 2 && (
            <p className="text-red-400 text-xs mt-1">Name must be at least 2 characters</p>
          )}
        </div>

        {/* Email */}
        <div>
          <label className="block text-sm font-medium text-gray-300 mb-2">
            Email
          </label>
          <div className="relative">
            <div className="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none">
              <Mail className="h-5 w-5 text-gray-400" />
            </div>
            <input
              type="email"
              value={formData.email}
              onChange={(e) => handleInputChange("email", e.target.value)}
              onKeyDown={handleKeyDown}
              className={`w-full pl-10 pr-4 py-3 bg-gray-800 border rounded-lg text-white placeholder-gray-400 focus:outline-none focus:ring-2 focus:border-transparent transition-all duration-300 hover:bg-gray-750 ${
                emailValid 
                  ? 'border-green-500 focus:ring-green-500' 
                  : formData.email.length > 0 
                  ? 'border-red-500 focus:ring-red-500' 
                  : 'border-gray-700 focus:ring-pink-500'
              }`}
              placeholder="Enter your email"
              required
            />
          </div>
          {formData.email.length > 0 && !emailValid && (
            <p className="text-red-400 text-xs mt-1">Please enter a valid email address</p>
          )}
        </div>

        {/* Password */}
        <div>
          <label className="block text-sm font-medium text-gray-300 mb-2">
            Password
          </label>
          <div className="relative">
            <div className="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none">
              <Lock className="h-5 w-5 text-gray-400" />
            </div>
            <input
              type={showPassword ? "text" : "password"}
              value={formData.password}
              onChange={(e) => handleInputChange("password", e.target.value)}
              onKeyDown={handleKeyDown}
              className="w-full pl-10 pr-12 py-3 bg-gray-800 border border-gray-700 rounded-lg text-white placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-pink-500 focus:border-transparent transition-all duration-300 hover:bg-gray-750"
              placeholder="Create a password"
              required
            />
            <button
              type="button"
              onClick={() => setShowPassword(!showPassword)}
              className="absolute inset-y-0 right-0 pr-3 flex items-center text-gray-400 hover:text-white transition-colors"
            >
              {showPassword ? <EyeOff className="h-5 w-5" /> : <Eye className="h-5 w-5" />}
            </button>
          </div>
          
          {/* Password Requirements */}
          {formData.password.length > 0 && (
            <div className="mt-2 space-y-1">
              <div className="flex items-center gap-2">
                {passwordValid.length ? (
                  <Check className="h-4 w-4 text-green-400" />
                ) : (
                  <X className="h-4 w-4 text-red-400" />
                )}
                <span className={`text-xs ${passwordValid.length ? 'text-green-400' : 'text-red-400'}`}>
                  At least 8 characters
                </span>
              </div>
              <div className="flex items-center gap-2">
                {passwordValid.uppercase ? (
                  <Check className="h-4 w-4 text-green-400" />
                ) : (
                  <X className="h-4 w-4 text-red-400" />
                )}
                <span className={`text-xs ${passwordValid.uppercase ? 'text-green-400' : 'text-red-400'}`}>
                  One uppercase letter
                </span>
              </div>
              <div className="flex items-center gap-2">
                {passwordValid.number ? (
                  <Check className="h-4 w-4 text-green-400" />
                ) : (
                  <X className="h-4 w-4 text-red-400" />
                )}
                <span className={`text-xs ${passwordValid.number ? 'text-green-400' : 'text-red-400'}`}>
                  One number
                </span>
              </div>
              <div className="flex items-center gap-2">
                {passwordValid.special ? (
                  <Check className="h-4 w-4 text-green-400" />
                ) : (
                  <X className="h-4 w-4 text-red-400" />
                )}
                <span className={`text-xs ${passwordValid.special ? 'text-green-400' : 'text-red-400'}`}>
                  One special character
                </span>
              </div>
            </div>
          )}
        </div>

        {/* Confirm Password */}
        <div>
          <label className="block text-sm font-medium text-gray-300 mb-2">
            Confirm Password
          </label>
          <div className="relative">
            <div className="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none">
              <Lock className="h-5 w-5 text-gray-400" />
            </div>
            <input
              type={showConfirmPassword ? "text" : "password"}
              value={formData.confirmPassword}
              onChange={(e) => handleInputChange("confirmPassword", e.target.value)}
              onKeyDown={handleKeyDown}
              className={`w-full pl-10 pr-12 py-3 bg-gray-800 border rounded-lg text-white placeholder-gray-400 focus:outline-none focus:ring-2 focus:border-transparent transition-all duration-300 hover:bg-gray-750 ${
                passwordsMatch 
                  ? 'border-green-500 focus:ring-green-500' 
                  : formData.confirmPassword.length > 0 
                  ? 'border-red-500 focus:ring-red-500' 
                  : 'border-gray-700 focus:ring-pink-500'
              }`}
              placeholder="Confirm your password"
              required
            />
            <button
              type="button"
              onClick={() => setShowConfirmPassword(!showConfirmPassword)}
              className="absolute inset-y-0 right-0 pr-3 flex items-center text-gray-400 hover:text-white transition-colors"
            >
              {showConfirmPassword ? <EyeOff className="h-5 w-5" /> : <Eye className="h-5 w-5" />}
            </button>
          </div>
          {formData.confirmPassword.length > 0 && !passwordsMatch && (
            <p className="text-red-400 text-xs mt-1">Passwords do not match</p>
          )}
        </div>

        <button
          type="submit"
          disabled={isLoading || !isFormValid()}
          className={`w-full py-3 px-4 rounded-lg font-semibold focus:outline-none focus:ring-2 focus:ring-pink-500 focus:ring-offset-2 focus:ring-offset-black transition-all duration-300 transform hover:scale-105 disabled:transform-none flex items-center justify-center gap-2 ${
            isFormValid() && !isLoading
              ? 'bg-gradient-to-r from-pink-500 to-purple-600 text-white hover:from-pink-600 hover:to-purple-700'
              : 'bg-gray-600 text-gray-300 cursor-not-allowed'
          }`}
        >
          {isLoading ? (
            <>
              <div className="animate-spin rounded-full h-5 w-5 border-b-2 border-white"></div>
              Creating Account...
            </>
          ) : (
            <>
              Sign Up
              <ArrowRight className="w-5 h-5" />
            </>
          )}
        </button>
      </form>

      <div className="mt-8 text-center">
        <p className="text-gray-400">
          Already have an account?{" "}
          <button 
            onClick={() => router.push("/login")}
            className="text-pink-400 hover:text-pink-300 font-semibold transition-colors"
          >
            Sign in
          </button>
        </p>
      </div>
    </>
  );
} 