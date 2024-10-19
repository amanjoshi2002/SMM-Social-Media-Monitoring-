/** @type {import('next').NextConfig} */
const nextConfig = {
  images: {
    domains: ['your-image-domain.com'], // Add any domains you're loading images from
  },
  env: {
    MONGODB_URI: process.env.MONGODB_URI,
  },
  // Add any other necessary configurations
};

export default nextConfig;
