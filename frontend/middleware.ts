import { withAuth } from "next-auth/middleware";
import { NextResponse } from "next/server";

export default withAuth(
  function middleware(req) {
    const { pathname } = req.nextUrl;
    const { token } = req.nextauth;

    console.log("Middleware: Pathname:", pathname, "Token:", !!token);

    // If user is authenticated and tries to access login/signup pages, redirect to home page
    if (token && (pathname === "/login" || pathname === "/signup")) {
      console.log("Middleware: Redirecting authenticated user from", pathname, "to /home");
      return NextResponse.redirect(new URL("/home", req.url));
    }

    // If user is authenticated and tries to access root path (landing page), redirect to home page
    if (token && pathname === "/") {
      console.log("Middleware: Redirecting authenticated user from / to /home");
      return NextResponse.redirect(new URL("/home", req.url));
    }

    // If user is not authenticated and tries to access protected pages, redirect to login
    if (!token && (pathname.startsWith("/home") || pathname.startsWith("/data"))) {
      console.log("Middleware: Redirecting unauthenticated user from", pathname, "to /login");
      return NextResponse.redirect(new URL("/login", req.url));
    }

    console.log("Middleware: Allowing access to", pathname);
    return NextResponse.next();
  },
  {
    callbacks: {
      authorized: ({ token }) => true, // We'll handle authorization in the middleware function
    },
  }
);

export const config = {
  matcher: [
    /*
     * Match all request paths except for the ones starting with:
     * - api (API routes)
     * - _next/static (static files)
     * - _next/image (image optimization files)
     * - favicon.ico (favicon file)
     * - public folder
     */
    "/((?!api|_next/static|_next/image|favicon.ico|public).*)",
  ],
};