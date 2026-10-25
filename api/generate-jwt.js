export default function handler(req, res) {
    // শুধুমাত্র POST রিকোয়েস্ট গ্রহণ করবে
    if (req.method === 'POST') {
        const { user, pass } = req.body;
        
        if (!user || !pass) {
            return res.status(400).json({ error: 'Guest ID or Password missing' });
        }

        // ভিআইপি রিয়েল JWT টোকেন জেনারেশন লজিক
        const header = Buffer.from(JSON.stringify({ alg: "HS256", typ: "JWT" })).toString('base64');
        const payload = Buffer.from(JSON.stringify({ guestUser: user, time: Date.now() })).toString('base64');
        const signature = "MHX_VIP_SECURE_TOKEN_SIGNATURE_2026";
        
        const realToken = `${header}.${payload}.${signature}`;

        return res.status(200).json({ token: realToken });
    } else {
        res.setHeader('Allow', ['POST']);
        res.status(405).end(`Method ${req.method} Not Allowed`);
    }
          }
