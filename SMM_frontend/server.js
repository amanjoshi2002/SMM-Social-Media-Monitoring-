const express = require('express');
const mongoose = require('mongoose');
const app = express();

// Connect to MongoDB Atlas
mongoose.connect('mongodb+srv://sctaman21:hellohello@test.vg3iafi.mongodb.net/test?retryWrites=true&w=majority&appName=test', {
    useNewUrlParser: true,
    useUnifiedTopology: true
});

// Create a schema for the notifications
const notificationSchema = new mongoose.Schema({
    platform: String,
    type: String,  // Added the type field
    url: String,
    createdAt: { type: Date, default: Date.now }
});

// Create a model from the schema
const Notification = mongoose.model('Notification', notificationSchema, 'urls');

// API endpoint to get notifications
app.get('/api/notifications', async (req, res) => {
    try {
        const notifications = await Notification.find().sort({ createdAt: -1 }).limit(10);
        res.json(notifications);
    } catch (error) {
        console.error('Error fetching notifications:', error);
        res.status(500).json({ error: 'An error occurred while fetching notifications' });
    }
});

// Start the server
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Server running on port ${PORT}`));
