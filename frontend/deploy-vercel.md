# Vercel Deployment Guide

This guide will help you deploy your Next.js frontend to Vercel.

## Prerequisites

1. **Vercel Account**: Sign up at [vercel.com](https://vercel.com)
2. **Git Repository**: Your code should be in a Git repository (GitHub, GitLab, or Bitbucket)
3. **Backend API URL**: Your Google Cloud Run backend URL

## Step 1: Prepare Your Repository

1. Make sure your frontend code is in a Git repository
2. Ensure all dependencies are in `package.json`
3. Verify your `next.config.ts` is properly configured

## Step 2: Deploy to Vercel

### Option A: Deploy via Vercel Dashboard (Recommended)

1. **Go to Vercel Dashboard**
   - Visit [vercel.com/dashboard](https://vercel.com/dashboard)
   - Click "New Project"

2. **Import Your Repository**
   - Connect your Git provider (GitHub, GitLab, Bitbucket)
   - Select your repository
   - Vercel will auto-detect it's a Next.js project

3. **Configure Project Settings**
   - **Framework Preset**: Next.js (should be auto-detected)
   - **Root Directory**: Leave empty (or specify if your frontend is in a subdirectory)
   - **Build Command**: `npm run build` (default)
   - **Output Directory**: `.next` (default)
   - **Install Command**: `npm install` (default)

4. **Set Environment Variables**
   - Click "Environment Variables"
   - Add the following:
     ```
     NEXT_PUBLIC_API_BASE_URL=https://your-cloud-run-url.run.app
     ```
   - Replace `your-cloud-run-url` with your actual Google Cloud Run URL

5. **Deploy**
   - Click "Deploy"
   - Vercel will build and deploy your application

### Option B: Deploy via Vercel CLI

1. **Install Vercel CLI**
   ```bash
   npm i -g vercel
   ```

2. **Login to Vercel**
   ```bash
   vercel login
   ```

3. **Deploy**
   ```bash
   cd frontend
   vercel
   ```

4. **Set Environment Variables**
   ```bash
   vercel env add NEXT_PUBLIC_API_BASE_URL
   # Enter your Google Cloud Run URL when prompted
   ```

## Step 3: Configure Custom Domain (Optional)

1. **Add Custom Domain**
   - Go to your project in Vercel Dashboard
   - Click "Settings" → "Domains"
   - Add your custom domain

2. **Configure DNS**
   - Follow Vercel's DNS configuration instructions
   - Add the required DNS records to your domain provider

## Step 4: Environment Variables

Make sure to set these environment variables in Vercel:

```bash
NEXT_PUBLIC_API_BASE_URL=https://your-cloud-run-url.run.app
```

## Step 5: Verify Deployment

1. **Check Your Deployment**
   - Visit your Vercel URL
   - Test all functionality
   - Check browser console for any errors

2. **Test API Integration**
   - Verify that your frontend can communicate with your Google Cloud Run backend
   - Test all API endpoints

## Troubleshooting

### Common Issues

1. **Build Failures**
   - Check the build logs in Vercel Dashboard
   - Ensure all dependencies are in `package.json`
   - Verify TypeScript compilation

2. **API Connection Issues**
   - Verify `NEXT_PUBLIC_API_BASE_URL` is set correctly
   - Check CORS configuration in your backend
   - Test API endpoints directly

3. **Environment Variables**
   - Ensure all environment variables are set in Vercel
   - Variables starting with `NEXT_PUBLIC_` are available in the browser

### Useful Commands

```bash
# Deploy to production
vercel --prod

# View deployment logs
vercel logs

# List all deployments
vercel ls

# Remove deployment
vercel remove
```

## Continuous Deployment

Once deployed, Vercel will automatically:
- Deploy new versions when you push to your main branch
- Create preview deployments for pull requests
- Provide rollback functionality

## Performance Optimization

1. **Enable Analytics**
   - Go to your project settings
   - Enable Vercel Analytics for performance insights

2. **Optimize Images**
   - Use Next.js Image component
   - Configure image optimization in `next.config.ts`

3. **Enable Caching**
   - Configure appropriate cache headers
   - Use Vercel's edge caching

## Security

1. **Environment Variables**
   - Never commit sensitive data to your repository
   - Use Vercel's environment variable system

2. **Headers**
   - The `vercel.json` includes security headers
   - Consider adding CSP headers if needed

## Support

- **Vercel Documentation**: [vercel.com/docs](https://vercel.com/docs)
- **Vercel Support**: [vercel.com/support](https://vercel.com/support)
- **Community**: [github.com/vercel/vercel/discussions](https://github.com/vercel/vercel/discussions)
