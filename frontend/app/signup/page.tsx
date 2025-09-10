import { Brain } from "lucide-react";
import { SignupForm } from "./SignupForm";
import { getServerSession } from "next-auth/next";
import { authOptions } from "@/lib/authOptions";
import { redirect } from "next/navigation";
import { Metadata } from "next";

export const metadata: Metadata = {
  title: "Sign Up - Clariverse",
  description: "Create your Clariverse account",
  robots: "noindex, nofollow",
};

export default async function SignupPage() {
  // Check if user is already authenticated
  const session = await getServerSession(authOptions);
  
  // If user is already logged in, redirect to home page
  if (session) {
    redirect("/home");
  }
  return (
    <div className="min-h-screen bg-black flex items-center justify-center px-4 py-8">
      <div className="w-full max-w-md">
        <div className="bg-gray-900 rounded-2xl p-8 shadow-2xl border border-gray-800">
          <div className="text-center mb-8">
            <div className="flex justify-center mb-4">
              <div className="w-16 h-16 rounded-full bg-gradient-to-r from-pink-500 to-purple-600 flex items-center justify-center">
                <Brain className="w-8 h-8 text-white" />
              </div>
            </div>

            <h1 className="text-3xl font-bold bg-gradient-to-r from-white to-gray-300 bg-clip-text text-transparent mb-2">
              Clariverse
            </h1>

            <h2 className="text-xl font-semibold text-white mb-2">
              Create Account
            </h2>

            <p className="text-gray-400">Sign up for a new account</p>
          </div>

          <SignupForm />

          <div className="mt-8 text-center">
            <p className="text-gray-500 text-sm">
              © 2025 Clariverse • All rights reserved
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}