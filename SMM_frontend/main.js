const { app, BrowserWindow, ipcMain } = require('electron')
const path = require('path')

let mainWindow;

function createWindow () {
  mainWindow = new BrowserWindow({
    width: 1024,
    height: 768,
    webPreferences: {
      nodeIntegration: true,
      contextIsolation: false
    }
  })

  mainWindow.loadFile('index.html')
}

app.whenReady().then(createWindow)

app.on('window-all-closed', () => {
  if (process.platform !== 'darwin') {
    app.quit()
  }
})

app.on('activate', () => {
  if (BrowserWindow.getAllWindows().length === 0) {
    createWindow()
  }
})

// Add these event listeners
ipcMain.on('navigate-back', (event) => {
  if (mainWindow.webContents.canGoBack()) {
    mainWindow.webContents.goBack()
  }
})

ipcMain.on('navigate-to', (event, url) => {
  mainWindow.loadFile(path.join(__dirname, url));
})
