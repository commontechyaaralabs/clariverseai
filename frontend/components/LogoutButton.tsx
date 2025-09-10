"use client";

import { signOut } from "next-auth/react";
import { LogOut } from "lucide-react";

export function LogoutButton() {
  return (
    <button
      onClick={() => signOut({ 
        callbackUrl: "/",
        redirect: true 
      })}
      className="flex items-center gap-2 bg-red-600 hover:bg-red-700 text-white px-4 py-2 rounded-lg transition-colors"
    >
      <LogOut className="w-4 h-4" />
      Logout
    </button>
  );
} 