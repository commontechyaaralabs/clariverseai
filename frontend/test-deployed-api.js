// Test script for deployed Cloud Run API
const testDeployedAPI = async () => {
  const apiUrl = 'https://topic-analysis-api-153115538723.us-central1.run.app';
  
  console.log('🧪 Testing deployed Cloud Run API...');
  console.log(`🌐 API URL: ${apiUrl}`);
  
  try {
    // Test health endpoint
    console.log('\n1. Testing health endpoint...');
    const healthResponse = await fetch(`${apiUrl}/health`);
    if (healthResponse.ok) {
      const healthData = await healthResponse.json();
      console.log('✅ Health check passed:', healthData);
    } else {
      console.log('❌ Health check failed:', healthResponse.status, healthResponse.statusText);
    }
    
    // Test root endpoint
    console.log('\n2. Testing root endpoint...');
    const rootResponse = await fetch(`${apiUrl}/`);
    if (rootResponse.ok) {
      const rootData = await rootResponse.json();
      console.log('✅ Root endpoint working:', rootData);
    } else {
      console.log('❌ Root endpoint failed:', rootResponse.status, rootResponse.statusText);
    }
    
    // Test stats endpoint
    console.log('\n3. Testing stats endpoint...');
    const statsResponse = await fetch(`${apiUrl}/api/v1/home/stats?data_type=email&domain=banking`);
    if (statsResponse.ok) {
      const statsData = await statsResponse.json();
      console.log('✅ Stats endpoint working:', statsData);
    } else {
      console.log('❌ Stats endpoint failed:', statsResponse.status, statsResponse.statusText);
    }
    
    console.log('\n🎉 API testing completed!');
    
  } catch (error) {
    console.error('❌ Error testing API:', error.message);
  }
};

testDeployedAPI();
