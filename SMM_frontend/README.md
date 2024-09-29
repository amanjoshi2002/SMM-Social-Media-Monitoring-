# Social Media Monitoring Frontend (SMM)

This project is the frontend for a Social Media Monitoring (SMM) tool built using **Electron** and **NPM**. The frontend provides an interface to monitor social media platforms, allowing users to retrieve data from X, Facebook, and Instagram. It interacts with the backend to fetch data, display sentiment analysis, and trigger alerts.

## Project Structure

- **`main.js`**: Main Electron process that handles window creation and app lifecycle.
- **`dashboard.html`**:Handles the dashboard components.
- **`index.html`**: The main UI layout for the application.
- **`styles.css`**: Contains the styling for the UI elements.
- **`hashtag-help.html`**: Handles the component which deal with help mentioned in the hashtags enabling police to prioritize that post.
- **`origin_track.html`**: Helps to trace the origin of a particular post based on graph.

## Tech Stack

- **Electron**: Provides the desktop application environment.
- **NPM**: Manages dependencies and scripts.

## Setup

### Prerequisites

- Node.js (v14 or higher) and NPM
- Electron

### Installation

2. **Install dependencies:**

    npm install

3. **Set up environment variables (if any):**

    If you need to communicate with the backend, ensure that your backend service is running and configured. You can adjust the backend URL in the frontend code as necessary.

## Usage

### Running the Frontend

To start the Electron app, run:

```sh
npm start
```
This should provide clear instructions on how to set up and run the project.
