This is a [Next.js](https://nextjs.org) project bootstrapped with [`create-next-app`](https://nextjs.org/docs/app/api-reference/cli/create-next-app).

## Authentication Setup

This application uses NextAuth.js for authentication with the following features:

- **Session Management**: 24-hour session persistence with secure cookies
- **Route Protection**: Middleware prevents authenticated users from accessing login/signup pages
- **Automatic Redirects**: Logged-in users are redirected to `/home`, unauthenticated users to `/login`

### Environment Variables

Create a `.env.local` file in the root directory with the following variables:

```bash
# NextAuth Configuration
NEXTAUTH_SECRET=your-secret-key-here
NEXTAUTH_URL=http://localhost:3000

# API Configuration
NEXT_PUBLIC_API_URL=https://clariversev1-153115538723.us-central1.run.app
```

### Authentication Flow

1. **Landing Page (`/`)**: 
   - Shows to unauthenticated users
   - Redirects authenticated users to `/home`
   - Contains "Get Started" and "Login" buttons
2. **Login Page (`/login`)**: 
   - Redirects authenticated users to landing page (`/`)
   - Back button goes to landing page
3. **Signup Page (`/signup`)**: 
   - Redirects authenticated users to landing page (`/`)
   - Back button goes to landing page
4. **Home Dashboard (`/home`)**: Protected route, requires authentication
5. **Logout**: Redirects to landing page (`/`)

## Getting Started

First, create your `.env.local` file with the required environment variables, then run the development server:

```bash
npm run dev
# or
yarn dev
# or
pnpm dev
# or
bun dev
```

Open [http://localhost:3000](http://localhost:3000) with your browser to see the result.

You can start editing the page by modifying `app/page.tsx`. The page auto-updates as you edit the file.

This project uses [`next/font`](https://nextjs.org/docs/app/building-your-application/optimizing/fonts) to automatically optimize and load [Geist](https://vercel.com/font), a new font family for Vercel.

## Learn More

To learn more about Next.js, take a look at the following resources:

- [Next.js Documentation](https://nextjs.org/docs) - learn about Next.js features and API.
- [Learn Next.js](https://nextjs.org/learn) - an interactive Next.js tutorial.

You can check out [the Next.js GitHub repository](https://github.com/vercel/next.js) - your feedback and contributions are welcome!

## Deploy on Vercel

The easiest way to deploy your Next.js app is to use the [Vercel Platform](https://vercel.com/new?utm_medium=default-template&filter=next.js&utm_source=create-next-app&utm_campaign=create-next-app-readme) from the creators of Next.js.

Check out our [Next.js deployment documentation](https://nextjs.org/docs/app/building-your-application/deploying) for more details.
