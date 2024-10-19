'use client'

import React, { useState, useCallback } from 'react';
import Image from 'next/image';
import { FaInstagram, FaTwitter, FaFacebook } from 'react-icons/fa';
import axios from 'axios';

export default function Home() {
  const [selectedPlatform, setSelectedPlatform] = useState('');
  const [inputType, setInputType] = useState('');
  const [urlInput, setUrlInput] = useState('');
  const [message, setMessage] = useState('');

  const handleIconClick = useCallback((platform) => {
    console.log('Icon clicked:', platform);
    setSelectedPlatform(platform);
    setInputType('');
    setMessage('');
  }, []);

  const handleTypeClick = useCallback((type) => {
    console.log('Type clicked:', type);
    setInputType(type);
    setMessage('');
  }, []);

  const handleSubmit = useCallback(async () => {
    if (!inputType) {
      setMessage('Please select a user or page type.');
      return;
    }
    if (!urlInput) {
      setMessage('Please enter a URL.');
      return;
    }
    try {
      const response = await axios.post('/api/submit-url', {
        platform: selectedPlatform,
        type: inputType,
        url: urlInput
      });
      setMessage(`URL submitted successfully for ${selectedPlatform} ${inputType}: ${urlInput}`);
      setUrlInput('');
    } catch (error) {
      console.error('Error submitting URL:', error);
      setMessage('Error submitting URL. Please try again.');
    }
  }, [selectedPlatform, inputType, urlInput]);

  const getIcon = (platform) => {
    switch (platform) {
      case 'instagram':
        return <FaInstagram size={30} color="#C13584" />;
      case 'twitter':
        return <FaTwitter size={30} color="#1DA1F2" />;
      case 'facebook':
        return <FaFacebook size={30} color="#4267B2" />;
      default:
        return null;
    }
  };

  return (
    <div style={{
      padding: '20px',
      backgroundColor: 'rgba(255, 255, 255, 0.8)',
      borderRadius: '15px',
      maxWidth: '500px',
      margin: '20px auto',
      textAlign: 'center'
    }}>
      <Image src="/img/logo.png" alt="Logo" width={100} height={100} style={{marginBottom: '20px'}} />
      <h1 style={{marginBottom: '20px'}}>Social Monitoring</h1>
      <div style={{display: 'flex', justifyContent: 'center', gap: '20px', marginBottom: '20px'}}>
        {['instagram', 'twitter', 'facebook'].map((platform) => (
          <button
            key={platform}
            onClick={() => handleIconClick(platform)}
            style={{
              background: 'none',
              border: 'none',
              cursor: 'pointer',
              padding: '10px',
              borderRadius: '50%',
              backgroundColor: selectedPlatform === platform ? 'rgba(0,0,0,0.1)' : 'transparent'
            }}
          >
            {getIcon(platform)}
          </button>
        ))}
      </div>
      {selectedPlatform && (
        <div style={{marginTop: '20px'}}>
          <button onClick={() => handleTypeClick('user')} style={{margin: '0 10px', padding: '10px 20px', backgroundColor: '#007bff', color: 'white', border: 'none', borderRadius: '5px'}}>User</button>
          <button onClick={() => handleTypeClick('page')} style={{margin: '0 10px', padding: '10px 20px', backgroundColor: '#007bff', color: 'white', border: 'none', borderRadius: '5px'}}>Page</button>
        </div>
      )}
      {inputType && (
        <div style={{marginTop: '20px'}}>
          <input
            type="url"
            placeholder={`Enter ${inputType} URL`}
            value={urlInput}
            onChange={(e) => setUrlInput(e.target.value)}
            style={{
              width: '100%',
              padding: '10px',
              marginBottom: '10px',
              borderRadius: '5px',
              border: '1px solid #ccc'
            }}
          />
          <button 
            onClick={handleSubmit}
            style={{
              padding: '10px 20px',
              backgroundColor: '#28a745',
              color: 'white',
              border: 'none',
              borderRadius: '5px',
              cursor: 'pointer'
            }}
          >
            Submit
          </button>
        </div>
      )}
      {message && <p style={{marginTop: '20px', color: 'red'}}>{message}</p>}
    </div>
  );
}
