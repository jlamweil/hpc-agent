import os
import sys

hero_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(hero_dir, "async-hermes-agent"))