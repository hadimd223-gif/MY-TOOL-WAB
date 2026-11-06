import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _core import app  # Vercel serves this file at /api/verifyPayment
