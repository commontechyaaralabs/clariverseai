// app/api/auth/[...nextauth]/authOptions.ts

import { NextAuthOptions } from "next-auth";
import CredentialsProvider from "next-auth/providers/credentials";

export const authOptions: NextAuthOptions = {
  providers: [
    CredentialsProvider({
      name: "Credentials",
      credentials: {
        email: { label: "Email", type: "text" },
        password: { label: "Password", type: "password" },
      },
      async authorize(credentials) {
        try {
          const apiBaseUrl = process.env.NEXT_PUBLIC_API_BASE_URL || "https://clariversev1-107731139870.us-central1.run.app";
          console.log("Auth: Attempting login with API URL:", apiBaseUrl);
          
          const res = await fetch(`${apiBaseUrl}/api/auth/login`, {
            method: "POST",
            body: JSON.stringify({
              email: credentials?.email,
              password: credentials?.password,
            }),
            headers: { "Content-Type": "application/json" },
          });

          console.log("Auth: Login response status:", res.status);

          if (!res.ok) {
            console.error("Login failed:", res.status, res.statusText);
            return null;
          }
          
          const response = await res.json() as { access_token?: string };
          console.log("Auth: Login response received:", !!response.access_token);

          if (response && response.access_token) {
            // Get user details using the access token
            const userRes = await fetch(`${apiBaseUrl}/api/auth/me`, {
              headers: {
                "Authorization": `Bearer ${response.access_token}`,
                "Content-Type": "application/json",
              },
            });

            console.log("Auth: User details response status:", userRes.status);

            if (userRes.ok) {
              const userData = await userRes.json();
              console.log("Auth: User data received:", userData);
              return {
                id: userData.id,
                name: userData.full_name,
                email: userData.email,
                accessToken: response.access_token,
              };
            }
          }
          return null;
        } catch (error) {
          console.error("Authentication error:", error);
          return null;
        }
      },
    }),
  ],
  callbacks: {
    async jwt({ token, user }: any) {
      if (user) {
        token.accessToken = user.accessToken;
      }
      return token;
    },
    async session({ session, token }: any) {
      session.accessToken = token.accessToken;
      return session;
    },
  },
  pages: {
    signIn: "/login",
  },
  session: {
    strategy: "jwt",
    maxAge: 24 * 60 * 60, // 24 hours
  },
  jwt: {
    maxAge: 24 * 60 * 60, // 24 hours
  },
  secret: process.env.NEXTAUTH_SECRET,
};
