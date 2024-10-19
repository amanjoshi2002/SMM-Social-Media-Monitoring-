import { NextResponse } from 'next/server';
import clientPromise from '@/lib/mongodb';

export async function POST(request) {
  try {
    const { platform, url, type } = await request.json();
    
    const client = await clientPromise;
    const db = client.db("test");
    
    const result = await db.collection("urls").insertOne({
      platform,
      url,
      type,
      createdAt: new Date()
    });

    return NextResponse.json({ message: 'URL submitted successfully', id: result.insertedId }, { status: 200 });
  } catch (error) {
    console.error('Error submitting URL:', error);
    return NextResponse.json({ message: 'Error submitting URL', error: error.message }, { status: 500 });
  }
}

export async function GET() {
  try {
    const client = await clientPromise;
    const db = client.db("socialMonitoring");
    
    const urls = await db.collection("urls").find({}).toArray();

    return NextResponse.json(urls, { status: 200 });
  } catch (error) {
    console.error('Error fetching URLs:', error);
    return NextResponse.json({ message: 'Error fetching URLs', error: error.message }, { status: 500 });
  }
}
