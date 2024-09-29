import React, { useState } from 'react';
import '../App.css';  // Import custom CSS for styling (optional)
import axios from 'axios';
import { FaInstagram, FaTwitter, FaFacebook } from 'react-icons/fa';
import { Button, Form, OverlayTrigger, Tooltip } from 'react-bootstrap';
import logo from '../img/logo.png'; // Import your logo

function App() {
  const [showList, setShowList] = useState(''); // Show list for user/page
  const [showInputForm, setShowInputForm] = useState(false); // Show input form
  const [inputType, setInputType] = useState(''); // To track whether user or page is selected
  const [urlInput, setUrlInput] = useState(''); // To track the URL input

  // Handle platform icon clicks
  const handleIconClick = (platform) => {
    setShowList(platform);
    setShowInputForm(false);
  };

  // Handle clicks on 'User' or 'Page' buttons
  const handleListClick = (type) => {
    setInputType(type);
    setShowInputForm(true); // Show the input form when clicked
  };

  // Handle the input field value change
  const handleInputChange = (e) => {
    setUrlInput(e.target.value);
  };

  // Handle form submission
  const handleSubmit = async () => {
    if (!inputType) {
      alert('Please select a user or page type.'); // Ensure type is selected
      return;
    }

    try {
      const response = await axios.post('http://localhost:5000/api/urls', {
        platform: showList,
        url: urlInput,
        type: inputType // Send the type (user or page) along with the request
      });
      alert(`Submitted ${inputType} URL: ${urlInput} for ${showList}`);
      setUrlInput(''); // Reset the input field
      setShowInputForm(false); // Hide the input form after submission
    } catch (error) {
      alert('Error submitting URL: ' + error.message);
    }
  };

  // Tooltip for icons
  const renderTooltip = (props) => (
    <Tooltip id="button-tooltip" {...props}>
      {props.children}
    </Tooltip>
  );

  return (
    <div className="App container mt-5 position-relative">
      <img src={logo} alt="Logo" className="logo" />
      <h2 className="text-center mb-4">Social Monitoring</h2>

      {/* Icons for Instagram, Twitter, Facebook */}
      <div className="d-flex justify-content-center">
        <OverlayTrigger placement="top" overlay={renderTooltip({ children: 'Instagram' })}>
          <div className="icon">
            <FaInstagram
              size={50}
              onClick={() => handleIconClick('instagram')}
              style={{ color: 'purple' }}
            />
          </div>
        </OverlayTrigger>
        
        <OverlayTrigger placement="top" overlay={renderTooltip({ children: 'Twitter' })}>
          <div className="icon">
            <FaTwitter
              size={50}
              onClick={() => handleIconClick('twitter')}
              style={{ color: 'skyblue' }}
            />
          </div>
        </OverlayTrigger>
        
        <OverlayTrigger placement="top" overlay={renderTooltip({ children: 'Facebook' })}>
          <div className="icon">
            <FaFacebook
              size={50}
              onClick={() => handleIconClick('facebook')}
              style={{ color: 'blue' }}
            />
          </div>
        </OverlayTrigger>
      </div>

      {/* List Popup for User and Page */}
      {showList && (
        <div className="text-center mt-4 button-container cont">
          <Button variant="secondary" onClick={() => handleListClick('user')}>
            User
          </Button>
          <Button variant="secondary" onClick={() => handleListClick('page')}>
            Page
          </Button>
        </div>
      )}

      {/* Inline Input Form */}
      {showInputForm && (
        <div className="mt-4">
          <h4 className="text-center">Enter {inputType === 'user' ? 'User' : 'Page'} URL</h4>
          <Form className="mt-3">
            <Form.Group controlId="formUrl">
              <Form.Label>{inputType.charAt(0).toUpperCase() + inputType.slice(1)} URL</Form.Label>
              <Form.Control
                type="url"
                placeholder={`Enter ${inputType} URL`}
                value={urlInput}
                onChange={handleInputChange}
                className="styled-input"
                required // Make the input field required
              />
            </Form.Group>
            <div className="text-center">
              <Button variant="primary" onClick={handleSubmit}>
                Submit
              </Button>
            </div>
          </Form>
        </div>
      )}
    </div>
  );
}

export default App;
