import clientPromise from '../../lib/mongodb';

export default async function handler(req, res) {
  if (req.method === 'POST') {
    try {
      const client = await clientPromise;
      const db = client.db("test");
      
      const { platform, type, url } = req.body;
      
      const result = await db.collection("urls").insertOne({
        platform,
        type,
        url,
        createdAt: new Date()
      });

      res.status(200).json({ message: "URL submitted successfully", id: result.insertedId });
    } catch (e) {
      console.error(e);
      res.status(500).json({ message: "Error submitting URL" });
    }
  } else {
    res.status(405).json({ message: "Method not allowed" });
  }
}

