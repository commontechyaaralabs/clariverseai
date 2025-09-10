// Simple test script to check backend connectivity
const testBackend = async () => {
  const backendUrls = [
    'https://clariversev1-153115538723.us-central1.run.app',
    'http://localhost:8000',
    'http://127.0.0.1:8000',
    'http://localhost:3001',
    'http://127.0.0.1:3001',
  ];

  for (const url of backendUrls) {
    try {
      console.log(`Testing ${url}...`);
      
      // Test health endpoint first
      const healthResponse = await fetch(`${url}/health`);
      if (healthResponse.ok) {
        console.log(`✅ Backend is running at ${url}`);
        
        // Test stats endpoint
        const statsResponse = await fetch(`${url}/api/v1/home/stats?data_type=email&domain=banking`);
        if (statsResponse.ok) {
          const data = await statsResponse.json();
          console.log(`✅ Stats endpoint working:`, data);
        } else {
          console.log(`❌ Stats endpoint failed: ${statsResponse.status} ${statsResponse.statusText}`);
        }
        return;
      }
    } catch (error) {
      console.log(`❌ ${url} not accessible:`, error.message);
    }
  }
  
  console.log('❌ No backend servers found. Please start the backend server.');
};

testBackend(); 