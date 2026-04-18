import sys
import os

HERO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(HERO_ROOT, "async-hermes-agent"))