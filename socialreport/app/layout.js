import './globals.css'
import 'bootstrap/dist/css/bootstrap.min.css'
import 'react-notifications/lib/notifications.css'

export const metadata = {
  title: 'Social Monitoring',
  description: 'Monitor social media platforms',
}

export default function RootLayout({ children }) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  )
}
