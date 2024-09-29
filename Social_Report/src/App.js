import React from 'react';
import Dashboard from './components/Dashboard';
import './App.css';  // Import custom CSS for styling (optional)
import axios from 'axios';


function App() {
  return (
    <div className="App">
      <Dashboard />
    </div>
  );
}

const handleSubmit = async () => {
  try {
    await axios.post('http://localhost:5000/api/urls', {
      platform: showList,
      url: urlInput
    });
    alert(`Submitted URL: ${urlInput} for ${inputType}`);
    setUrlInput('');  // Reset the input field
    setShowInputForm(false);  // Hide the input form after submission
  } catch (error) {
    alert('Error submitting URL: ' + error.message);
  }
};

export default App;
